from __future__ import annotations

import asyncio
import inspect
import types
import uuid
import weakref
from collections.abc import AsyncGenerator as AsyncGeneratorABC
from collections.abc import Awaitable as AwaitableABC
from collections.abc import Coroutine as CoroutineABC
from collections.abc import Generator as GeneratorABC
from enum import IntEnum
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from fastapi import Request
from fastapi.params import Depends
from loguru import logger
from starlette.types import ASGIApp, Receive, Scope, Send

T = TypeVar("T")

# Factory (ctor) may return:
# - a plain instance T
# - an Awaitable[T] (async def or sync returning awaitable)
# - a synchronous Generator[T, Any, Any]
# - an asynchronous AsyncGenerator[T, Any]
Ctor = Callable[..., Union[
    T,
    Awaitable[T],
    GeneratorABC[T, Any, Any],
    AsyncGeneratorABC[T, Any],
]]

# Destructor (dtor): takes an instance T, may be sync or async
Dtor = Optional[Callable[[T], Union[None, Awaitable[None]]]]


class ServiceLifetime(IntEnum):
    """
    Lifetime of a registered service.

    SINGLETON:
        A single instance is created on first access and reused afterwards.
    TRANSIENT:
        A new instance is created for each request/resolution.
    """

    SINGLETON = 0
    TRANSIENT = 1


class ServiceContainer:
    """
    Dependency container supporting key-based and type-based resolution,
    designed for async frameworks such as FastAPI.

    Concurrency model
    -----------------
    * The container is intended to be used **within a single asyncio event loop**.
    * All write operations (`register()`, `destruct_all_singletons()`) are
      serialized via an internal async lock (`self._lock`).
      The body of `register()` does **not await** while holding the lock, so
      updates are atomic with respect to other coroutines in the same loop.
    * Read operations (`aget()`, `aget_by_type()`) are lock-free for better
      throughput, but guarded by a lightweight event-loop consistency check.

    IMPORTANT:
    ----------
    * The container is **not thread-safe** and must not be shared across
      multiple event loops or OS threads.
    * While `destruct_all_singletons()` is running, `self.destructing` is set,
      and singleton resolution will fail fast with a clear error.
    """

    class SingletonService:
        """
        Internal representation of a singleton service.

        The instance is created once and cached.
        """

        __slots__ = (
            "_container_ref",
            "ctor",
            "dtor",
            "service_type",
            "public_key",
            "internal_id",
            "instance",
            "ctor_args",
            "ctor_kwargs",
            "_async_lock",
        )

        def __init__(
                self,
                container: ServiceContainer,
                ctor: Ctor,
                dtor: Dtor,
                service_type: Any,
                public_key: Optional[str],
                internal_id: str,
                *args: Any,
                **kwargs: Any,
        ) -> None:
            self._container_ref: weakref.ReferenceType[ServiceContainer] = weakref.ref(
                container)
            self.ctor: Ctor = ctor
            self.dtor: Dtor = dtor
            self.service_type: Any = service_type
            self.public_key: Optional[str] = public_key
            self.internal_id: str = internal_id

            self.instance: Any = None
            self.ctor_args: tuple[Any, ...] = args
            self.ctor_kwargs: dict[str, Any] = kwargs

            # Async lock for single-instance creation.
            self._async_lock = asyncio.Lock()

        def _ensure_container_active(self) -> None:
            """
            Ensure the owning container still exists and is not in destruction.
            Also enforces that we are running on the container's original loop.
            """
            container = self._container_ref()
            if container is None:
                msg = "Requesting service from a destroyed container."
                logger.error(msg)
                raise RuntimeError(msg)
            # Enforce single-loop usage.
            container._ensure_event_loop()
            if container.destructing:
                msg = "Requesting service from a destructing container."
                logger.error(msg)
                raise RuntimeError(msg)

        async def async_create_instance(self) -> None:
            """
            Create the singleton instance in an async context.

            - Coroutine factories are awaited directly.
            - Synchronous factories are executed in a background thread
              to avoid blocking the event loop.
            - Generator / async generator factories are rejected
              (use TRANSIENT lifetime for those).
            """
            if self.instance is not None:
                return

            async with self._async_lock:
                if self.instance is not None:
                    return

                self._ensure_container_active()

                if inspect.iscoroutinefunction(self.ctor):
                    result = await self.ctor(*self.ctor_args, **self.ctor_kwargs)
                else:
                    # Run sync factories in a separate thread to avoid blocking the loop.
                    result = await asyncio.to_thread(
                        self.ctor,
                        *self.ctor_args,
                        **self.ctor_kwargs,
                    )
                    if inspect.isawaitable(result):
                        result = await result

                if inspect.isgenerator(result) or inspect.isasyncgen(result):
                    msg = (
                        "Generator/async-generator factory not allowed for "
                        "SINGLETON services. Use TRANSIENT lifetime instead."
                    )
                    logger.error(msg)
                    raise RuntimeError(msg)

                self.instance = result

    class TransientService:
        """
        Internal representation of a transient service.

        A new instance is created on each resolution.
        """

        __slots__ = (
            "ctor",
            "dtor",
            "service_type",
            "public_key",
            "internal_id",
        )

        def __init__(
                self,
                ctor: Ctor,
                dtor: Dtor,
                service_type: Any,
                public_key: Optional[str],
                internal_id: str,
        ) -> None:
            self.ctor: Ctor = ctor
            self.dtor: Dtor = dtor
            self.service_type: Any = service_type
            self.public_key: Optional[str] = public_key
            self.internal_id: str = internal_id

    # Type alias for internal service objects.
    Service = Union[SingletonService, TransientService]

    def __init__(self) -> None:
        import os

        # internal_id -> service
        self._services: dict[str, ServiceContainer.Service] = {}
        # public key -> internal_id
        self._key_index: dict[str, str] = {}
        # service_type -> set[internal_id]
        self._type_index: dict[Any, set[str]] = {}

        # Guard against cross-loop / cross-thread access
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Guard against cross-process access
        self._pid: int = os.getpid()

        self.destructing: bool = False
        self._lock = asyncio.Lock()

    # --------------------------------------------------------------------- #
    # Internal helpers                                                      #
    # --------------------------------------------------------------------- #

    def _ensure_same_process(self) -> None:
        """
        Ensure the container is used from the same process it was created in.

        This prevents issues in multi-process deployments (e.g., Gunicorn workers)
        where each worker should have its own container instance.
        """
        import os
        current_pid = os.getpid()
        if self._pid != current_pid:
            msg = (
                f"ServiceContainer accessed from different process "
                f"(created in PID {self._pid}, accessed from PID {current_pid}). "
                f"Each process must have its own ServiceContainer instance."
            )
            logger.error(msg)
            raise RuntimeError(msg)

    def _ensure_event_loop(self) -> asyncio.AbstractEventLoop:
        """
        Ensure the container is always used from the same event loop.

        This is a best-effort runtime check to catch accidental cross-loop
        access early rather than failing in subtle ways later.
        """
        # Check process isolation first
        self._ensure_same_process()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:  # pragma: no cover - defensive
            msg = "ServiceContainer methods must be used within an asyncio event loop."
            logger.error(msg)
            raise RuntimeError(msg) from exc

        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            msg = (
                "ServiceContainer used from multiple event loops; "
                "this is not supported. Create a separate container per loop."
            )
            logger.error(msg)
            raise RuntimeError(msg)

        return loop

    @staticmethod
    def _infer_service_type(ctor: Ctor) -> Any:
        """
        Infer the logical "service type" from the factory's return annotation.

        For non-generator factories:
            - async def f() -> MyType: service type is MyType
            - def f() -> MyType: service type is MyType
            - def f() -> Awaitable[MyType] / Coroutine[..., MyType]:
              service type is MyType

        For generator factories:
            - def f() -> Generator[MyType, Any, Any]: service type is MyType

        For async generator factories:
            - async def f() -> AsyncGenerator[MyType, Any]: service type is MyType

        If no meaningful annotation can be determined, returns None.

        The implementation is defensive and tries to be stable across Python
        versions and different typing styles (Annotated, Optional, | None, etc.).
        """
        try:
            sig = inspect.signature(ctor)
        except (TypeError, ValueError):
            return None

        ann = sig.return_annotation
        if ann is inspect.Signature.empty:
            return None

        # Resolve forward references / string annotations / Annotated
        try:
            hints = get_type_hints(ctor, include_extras=True)
            ann = hints.get("return", ann)
        except Exception:
            # Type hints are best-effort only; never break runtime on failures.
            pass

        # Unwrap Annotated[T, ...]
        if get_origin(ann) is Annotated:
            args = get_args(ann)
            ann = args[0] if args else None
            if ann is None:
                return None
            # Check if unwrapped type is a string forward reference
            if isinstance(ann, str):
                logger.warning(
                    f"Unable to resolve forward reference '{ann}' in Annotated type. "
                    "For anonymous registration, ensure all type annotations are resolvable."
                )
                return None

        origin = get_origin(ann)

        # Unwrap Optional[T] / Union[T, None] / T | None → T
        if origin is Union or origin is types.UnionType:
            u_args = [a for a in get_args(ann) if a is not type(None)]
            if len(u_args) == 1:
                ann = u_args[0]
                # Check if unwrapped type is a string forward reference
                if isinstance(ann, str):
                    logger.warning(
                        f"Unable to resolve forward reference '{ann}' in Optional/Union type. "
                        "For anonymous registration, ensure all type annotations are resolvable."
                    )
                    return None
                origin = get_origin(ann)

        # AsyncGenerator[T, ...] / Generator[T, ...]
        if origin in (AsyncGenerator, AsyncGeneratorABC, GeneratorABC):
            args = get_args(ann)
            if args:
                inner_type = args[0]
                # Reject unresolved string forward references
                if isinstance(inner_type, str):
                    logger.warning(
                        f"Unable to resolve forward reference '{inner_type}' in generator type. "
                        "For anonymous registration, ensure all type annotations are resolvable."
                    )
                    return None
                return inner_type
            return None

        # Awaitable[T]
        if origin is AwaitableABC:
            args = get_args(ann)
            if args:
                inner_type = args[0]
                # Reject unresolved string forward references
                if isinstance(inner_type, str):
                    logger.warning(
                        f"Unable to resolve forward reference '{inner_type}' in Awaitable type. "
                        "For anonymous registration, ensure all type annotations are resolvable."
                    )
                    return None
                logger.warning(
                    "Return annotation uses Awaitable[T]; prefer annotating async "
                    "factories as `-> T`. Using T as service type."
                )
                return inner_type

        # Coroutine[Any, Any, T]
        if origin is CoroutineABC:
            args = get_args(ann)
            if len(args) == 3:
                inner_type = args[2]
                # Reject unresolved string forward references
                if isinstance(inner_type, str):
                    logger.warning(
                        f"Unable to resolve forward reference '{inner_type}' in Coroutine type. "
                        "For anonymous registration, ensure all type annotations are resolvable."
                    )
                    return None
                logger.warning(
                    "Return annotation uses Coroutine[..., T]; prefer annotating async "
                    "factories as `-> T`. Using T as service type."
                )
                return inner_type

        # Non-generic: treat the annotation itself as the service type.
        # Reject string annotations (unresolved forward references) to prevent
        # accidental registration with string keys instead of actual types.
        if isinstance(ann, str):
            logger.warning(
                f"Unable to resolve forward reference '{ann}' to an actual type. "
                "For anonymous registration, ensure all type annotations are resolvable."
            )
            return None

        return ann

    def _register_type_index(
            self,
            service_type: Any,
            internal_id: str,
            public_key: Optional[str],
    ) -> None:
        """
        Update internal type index and enforce registration rules for a given type.

        Anonymous registration:
            - Fails if any service (anonymous or named) of the same type already exists.
            - Fails if service_type is None (cannot infer type).

        Named registration:
            - Fails if an anonymous service of the same type already exists.
        """
        # Anonymous registration requires a valid service type
        if public_key is None and service_type is None:
            msg = (
                "Anonymous service registration failed: unable to infer service type. "
                "Please provide a type annotation on the factory function or use a named registration."
            )
            logger.error(msg)
            raise TypeError(msg)

        if service_type is None:
            return

        existing_ids = self._type_index.get(service_type, set())

        if public_key is None:
            # Anonymous registration: must be unique for this logical type.
            if existing_ids:
                msg = (
                    f"Anonymous registration for type {service_type!r} is not allowed: "
                    f"a service of this type already exists."
                )
                logger.error(msg)
                raise RuntimeError(msg)
        else:
            # Named registration: cannot coexist with an anonymous one of same type.
            for sid in existing_ids:
                svc = self._services.get(sid)
                if svc is not None and getattr(svc, "public_key", None) is None:
                    msg = (
                        f"Cannot register named service '{public_key}' for type {service_type!r}: "
                        f"a unique anonymous service of this type already exists."
                    )
                    logger.error(msg)
                    raise RuntimeError(msg)

        if service_type not in self._type_index:
            self._type_index[service_type] = set()
        self._type_index[service_type].add(internal_id)

    def _get_service_by_key(self, key: str) -> Service:
        """
        Resolve a service by its public key.

        Raises RuntimeError if no service is registered with the given key.
        """
        internal_id = self._key_index.get(key)
        if internal_id is None:
            msg = f"Requesting unregistered service: {key}"
            logger.error(msg)
            raise RuntimeError(msg)
        return self._services[internal_id]

    def _get_service_by_type(self, service_type: Any) -> Service:
        """
        Resolve a service by its registered type.

        Raises RuntimeError if:
            - no service is registered for the given type; or
            - multiple services share the same type.
        """
        ids = self._type_index.get(service_type)
        if not ids:
            msg = f"No service registered for type: {service_type!r}"
            logger.error(msg)
            raise RuntimeError(msg)
        if len(ids) > 1:
            msg = (
                f"Multiple services registered for type {service_type!r}; "
                f"use key-based injection instead."
            )
            logger.error(msg)
            raise RuntimeError(msg)
        internal_id = next(iter(ids))
        return self._services[internal_id]

    @staticmethod
    def _make_async_finalizer(
            dtor: Callable[[Any], Any],
            instance: Any,
    ) -> Callable[[], Awaitable[None]]:
        """
        Wrap a destructor into an async callable that can be awaited.

        The destructor itself may be synchronous or return an awaitable.
        """

        async def _finalizer() -> None:
            result = dtor(instance)
            if inspect.isawaitable(result):
                await result

        return _finalizer

    async def _aget_impl(
            self,
            service: Service,
            request: Optional[Request],
            key_label: str,
            *args: Any,
            **kwargs: Any,
    ) -> Any:
        """
        Core async resolution logic shared by key-based and type-based resolution.
        """
        # Enforce single-loop usage for all resolution paths.
        self._ensure_event_loop()

        if isinstance(service, ServiceContainer.SingletonService):
            # Singleton resolution.
            if args or kwargs:
                logger.warning(
                    f"Arguments given for singleton service '{key_label}' are ignored."
                )
            if self.destructing:
                msg = f"Requesting service '{key_label}' while container is destructing."
                logger.error(msg)
                raise RuntimeError(msg)
            if service.instance is not None:
                return service.instance

            await service.async_create_instance()
            logger.debug(f"Singleton service created: {key_label}")
            return service.instance

        # Transient resolution.
        ctor = service.ctor
        dtor = service.dtor
        finalizer: Optional[Callable[[], Awaitable[None]]] = None

        # Execute factory: async directly, sync in a background thread to avoid blocking.
        if inspect.iscoroutinefunction(ctor):
            result = await ctor(*args, **kwargs)
        else:
            # Run sync factories in a separate thread to avoid blocking the loop.
            result = await asyncio.to_thread(ctor, *args, **kwargs)
            if inspect.isawaitable(result):
                result = await result

        # Async generator.
        if inspect.isasyncgen(result):
            agen = result
            try:
                instance = await agen.__anext__()
            except StopAsyncIteration:
                raise RuntimeError(
                    f"Async generator service '{key_label}' did not yield a value."
                )

            async def _close_gen() -> None:
                await agen.aclose()

            if dtor:
                logger.warning(
                    f"Async generator service '{key_label}' should use `async with` or "
                    f"`yield ... finally` for cleanup instead of a separate destructor."
                )
                dtor_finalizer = self._make_async_finalizer(dtor, instance)

                async def _finalizer() -> None:
                    try:
                        await dtor_finalizer()
                    finally:
                        await _close_gen()

                finalizer = _finalizer
            else:
                finalizer = _close_gen

        # Synchronous generator.
        elif inspect.isgenerator(result):
            gen = result
            try:
                instance = next(gen)
            except StopIteration:
                msg = f"Generator service '{key_label}' did not yield a value."
                logger.error(msg)
                raise RuntimeError(msg)

            async def _close_gen() -> None:
                await asyncio.to_thread(gen.close)

            if dtor:
                logger.warning(
                    f"Generator service '{key_label}' should use `with` or "
                    f"`yield ... finally` for cleanup instead of a separate destructor."
                )
                dtor_finalizer = self._make_async_finalizer(dtor, instance)

                async def _finalizer() -> None:
                    try:
                        await dtor_finalizer()
                    finally:
                        await _close_gen()

                finalizer = _finalizer
            else:
                finalizer = _close_gen

        # Plain object.
        else:
            instance = result
            if dtor:
                finalizer = self._make_async_finalizer(dtor, instance)

        if finalizer is not None:
            self._attach_finalizer_to_request(request, finalizer)

        return instance

    # --------------------------------------------------------------------- #
    # Public API                                                            #
    # --------------------------------------------------------------------- #

    async def register(
            self,
            key: Optional[str],
            lifetime: ServiceLifetime,
            ctor: Ctor,
            dtor: Dtor,
            *args: Any,
            **kwargs: Any,
    ) -> None:
        """
        Register a service in the container.

        This method is safe to call at runtime from within the same asyncio
        event loop. Registrations are serialized by an internal async lock.

        Parameters
        ----------
        key:
            Public key for the service. If None, the service is anonymous and
            can only be resolved by type. See class docstring for detailed rules.
        lifetime:
            Service lifetime (singleton or transient).
        ctor:
            Callable that creates the service instance. It may be synchronous,
            asynchronous, generator-based, or async generator-based depending on
            the lifetime and usage.
        dtor:
            Optional destructor called when the service is torn down.
            For singletons, it is invoked by `destruct_all_singletons`.
            For transient services, it is invoked at the end of the request
            that created the instance.
        args, kwargs:
            Additional arguments passed to the factory for singleton services.
            For transient services, arguments must be supplied at resolution
            time; registration-time arguments are ignored with a warning.
        """
        # Enforce single-loop usage for all writes.
        self._ensure_event_loop()

        async with self._lock:
            public_key = key
            internal_id = uuid.uuid4().hex

            if self.destructing:
                msg = "Cannot register services while container is destructing."
                logger.error(msg)
                raise RuntimeError(msg)

            if public_key is not None and public_key in self._key_index:
                msg = f"Duplicate service registration for key: {public_key}"
                logger.error(msg)
                raise RuntimeError(msg)

            if dtor is not None and not callable(dtor):
                msg = (
                    f"Invalid destructor for service key={public_key!r}: "
                    f"expected a callable or None, got {type(dtor)!r}."
                )
                logger.error(msg)
                raise TypeError(msg)

            service_type = self._infer_service_type(ctor)

            if lifetime == ServiceLifetime.SINGLETON:
                service: ServiceContainer.Service = ServiceContainer.SingletonService(
                    self,
                    ctor,
                    dtor,
                    service_type,
                    public_key,
                    internal_id,
                    *args,
                    **kwargs,
                )
            else:
                if args or kwargs:
                    logger.warning(
                        "Arguments provided when registering a transient service are ignored."
                    )
                service = ServiceContainer.TransientService(
                    ctor,
                    dtor,
                    service_type,
                    public_key,
                    internal_id,
                )

            # Update indices first to avoid half-registered services on error.
            # This will raise an error if anonymous registration has no type
            self._register_type_index(service_type, internal_id, public_key)

            if public_key is not None:
                self._key_index[public_key] = internal_id

            # Finally insert into services table.
            self._services[internal_id] = service

            logger.debug(
                f"Service registered: key={public_key}, type={service_type}, id={internal_id}"
            )

    def request_kwarg_name(self) -> str:
        """
        Name of the keyword argument used when passing the Request object
        into aget/aget_by_type calls, for attaching transient finalizers.
        """
        return f"_svc_request_{id(self)}"

    def _request_ctx_key(self) -> str:
        """
        Name of the attribute on request.state used to store per-request data.

        A unique name based on the container id is used to avoid collisions.
        """
        return f"_svc_ctx_{id(self)}"

    def _get_or_create_request_ctx(self, request: Request) -> dict[str, Any]:
        """
        Return the per-request context dictionary for this container.

        Structure:
            {
                "transient_finalizers": list[Callable[[], Awaitable[None]]]
            }
        """
        key = self._request_ctx_key()
        ctx = getattr(request.state, key, None)
        if ctx is None:
            ctx = {"transient_finalizers": []}
            setattr(request.state, key, ctx)
        return ctx

    def _attach_finalizer_to_request(
            self,
            request: Optional[Request],
            finalizer: Callable[[], Awaitable[None]],
    ) -> None:
        """
        Attach a transient finalizer to the current request.

        If request is None (e.g. resolution outside a request context),
        a warning is logged and the finalizer is not tracked.
        """
        if request is None:
            logger.warning(
                "No request context provided: transient finalizer cannot be attached "
                "(resource may leak)."
            )
            return
        ctx = self._get_or_create_request_ctx(request)
        ctx["transient_finalizers"].append(finalizer)

    async def aget_by_key(self, key: str, *args: Any, **kwargs: Any) -> Any:
        """
        Asynchronously resolve a service instance by key.

        This method is intended for internal or low-level use. In FastAPI
        endpoints, the `Inject()` helper should be preferred.
        """
        self._ensure_event_loop()

        _request_key = self.request_kwarg_name()
        request: Optional[Request] = kwargs.pop(_request_key, None)

        service = self._get_service_by_key(key)
        key_label = key
        return await self._aget_impl(service, request, key_label, *args, **kwargs)

    async def aget_by_type(self, service_type: Type[Any], *args: Any, **kwargs: Any) -> Any:
        """
        Asynchronously resolve a service instance by its registered type.

        Type-based resolution is only allowed when exactly one service of the
        given type is registered.
        """
        self._ensure_event_loop()

        _request_key = self.request_kwarg_name()
        request: Optional[Request] = kwargs.pop(_request_key, None)

        service = self._get_service_by_type(service_type)
        label = service.public_key or f"<type:{service_type!r}>"
        return await self._aget_impl(service, request, label, *args, **kwargs)

    async def destruct_all_singletons(self) -> None:
        """
        Destroy all singleton instances in reverse registration order.

        Each singleton's destructor (if any) is invoked and the instance
        reference is cleared. This method is idempotent and safe to call
        multiple times.

        While this method is running, the internal lock is held, so concurrent
        registration attempts will block until destruction finishes.
        """
        self._ensure_event_loop()

        async with self._lock:
            if self.destructing:
                return
            self.destructing = True
            logger.debug("Starting destruction of all singleton services...")

            try:
                for service in reversed(list(self._services.values())):
                    if (
                            isinstance(service, ServiceContainer.SingletonService)
                            and service.instance is not None
                    ):
                        label = service.public_key or service.internal_id
                        try:
                            if service.dtor:
                                finalizer = self._make_async_finalizer(
                                    service.dtor,
                                    service.instance,
                                )
                                await finalizer()
                        except Exception:
                            logger.exception(
                                f"Error destructing singleton service: {label}")
                        finally:
                            service.instance = None
                            logger.debug(
                                f"Singleton instance released: {label}")
            finally:
                self.destructing = False
                logger.debug("Finished destruction of all singleton services.")

    def require(self, key: str, *, allow_transient: bool = False) -> Callable[..., Awaitable[Any]]:
        """
        Declare a dependency on another registered service by key.

        Returns a lazy async lambda that resolves the service when awaited.
        This is intended for wiring dependencies between services themselves,
        not for FastAPI endpoint resolution.
        """
        service = self._get_service_by_key(key)
        if not allow_transient and isinstance(service, ServiceContainer.TransientService):
            raise RuntimeError(
                f"Service '{key}' is transient. "
                "Singletons should not depend on transient services "
                "(set allow_transient=True to override)."
            )

        return lambda *a, **kw: self.aget_by_key(key, *a, **kw)


def Inject(
        target: Any = None,
        *args: Any,
        **kwargs: Any,
) -> Depends:
    """
    Create a FastAPI dependency marker for a registered service.

    In endpoints you can write:

        @router.get("/items")
        async def endpoint(db: DbConn = Inject(DbConn)):
            ...

    Resolution modes
    ----------------
    1. Key-based resolution:
        - Inject("my_key")

    2. Type-based resolution:
        - Inject(MyType)

       This requires that exactly one service of type MyType is registered.
    """
    # Decide lookup mode.
    if isinstance(target, str):
        key_specified = True
        lookup_value = target
    elif isinstance(target, type):
        key_specified = False
        lookup_value = target
    else:
        raise TypeError(
            "Inject() expects either a service key (str) or a service type."
        )

    # Build the dependency signature.
    params = [
        inspect.Parameter(
            "request",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Request,
        )
    ]
    pos_dep_map: dict[int, str] = {}
    pos_static: dict[int, Any] = {}
    kw_dep_map: dict[str, str] = {}
    kw_static: dict[str, Any] = {}

    for i, a in enumerate(args):
        if isinstance(a, Depends):
            name = f"_dep_arg_{i}_{uuid.uuid4().hex[:8]}"
            params.append(
                inspect.Parameter(
                    name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=a,
                )
            )
            pos_dep_map[i] = name
        else:
            pos_static[i] = a

    for k, v in kwargs.items():
        if isinstance(v, Depends):
            name = f"_dep_kw_{k}_{uuid.uuid4().hex[:8]}"
            params.append(
                inspect.Parameter(
                    name,
                    inspect.Parameter.KEYWORD_ONLY,
                    default=v,
                )
            )
            kw_dep_map[k] = name
        else:
            kw_static[k] = v

    sig = inspect.Signature(params)

    async def _dependency_callable(request: Request, **resolved_deps: Any) -> Any:
        services: Optional[ServiceContainer] = getattr(
            request.app.state, "services", None)
        if services is None:
            msg = "Service container not initialized on FastAPI app state."
            logger.error(msg)
            raise RuntimeError(msg)

        # Reconstruct positional arguments.
        final_args = [
            resolved_deps[pos_dep_map[i]
            ] if i in pos_dep_map else pos_static[i]
            for i in range(len(args))
        ]
        # Reconstruct keyword arguments.
        final_kwargs = {
            k: resolved_deps[kw_dep_map[k]
            ] if k in kw_dep_map else kw_static[k]
            for k in kwargs
        }

        # Attach request for tracking transient finalizers.
        final_kwargs[services.request_kwarg_name()] = request

        if key_specified:
            return await services.aget_by_key(lookup_value, *final_args, **final_kwargs)
        else:
            return await services.aget_by_type(lookup_value, *final_args, **final_kwargs)

    # Improve callable name for better error stacks & docs
    if key_specified:
        name_suffix = str(lookup_value)
    else:
        name_suffix = getattr(lookup_value, "__name__", repr(lookup_value))

    _dependency_callable.__name__ = (
        f"inject_{'key' if key_specified else 'type'}_{name_suffix}"
    )
    _dependency_callable.__qualname__ = _dependency_callable.__name__
    _dependency_callable.__signature__ = sig

    return Depends(_dependency_callable)


class TransientServiceFinalizerMiddleware:
    """
    ASGI middleware that runs transient service finalizers at the end of each HTTP request.

    This middleware should be added to the ASGI app after all other middlewares that may resolve transient services.

    Usage:

        app.add_middleware(TransientServiceFinalizerMiddleware)
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def _run_finalizers(self, scope: Scope) -> None:
        app_state = getattr(scope.get("app"), "state", None)
        services = getattr(app_state, "services", None) if app_state else None
        if services is None:
            return

        # scope["state"] is a plain dict, not the State wrapper object.
        # We must use dict operations directly.
        state = scope.get("state")
        if state is None:
            return
        ctx_key = services._request_ctx_key()
        ctx = state.get(ctx_key)
        if not ctx:
            return
        finalizers = ctx.get("transient_finalizers", [])
        for finalizer in reversed(finalizers):
            try:
                await finalizer()
            except Exception:
                logger.exception("Error running transient finalizer.")
        # Clean up the context from the state dict
        state.pop(ctx_key, None)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        finalizers_run = False

        async def send_wrapper(message):
            nonlocal finalizers_run
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                await self._run_finalizers(scope)
                finalizers_run = True
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            if not finalizers_run:
                await self._run_finalizers(scope)
