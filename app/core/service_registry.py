from __future__ import annotations

import asyncio
import importlib
import inspect
import pkgutil
import threading
from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Optional

from loguru import logger

from app.core.dependencies import ServiceContainer, ServiceLifetime

Ctor = Callable[..., Any]
Dtor = Optional[Callable[[Any], Any]]
LifetimeLike = ServiceLifetime | int | Literal[
    "Singleton",
    "Transient",
    "singleton",
    "transient",
]
ExpandedDefinition = tuple[
    "_ServiceDefinition",
    str,
    dict[str, Any],
    dict[str, "RequiredService"],
    inspect.Signature,
]
ResolvedByKey = tuple[
    "_ServiceDefinition",
    dict[str, "RequiredService"],
    inspect.Signature,
    dict[str, Any],
    Any,
]
ResolvedSpec = tuple[
    "_ServiceDefinition",
    inspect.Signature,
    dict[str, Any],
    tuple["_ResolvedDependency", ...],
    Any,
]


@dataclass(frozen=True)
class RequiredService:
    target: str | type[Any]
    allow_transient: bool = False

    def render_for_dict_key(self, dict_key: str) -> RequiredService:
        if isinstance(self.target, str) and "{}" in self.target:
            return RequiredService(
                target=self.target.replace("{}", str(dict_key)),
                allow_transient=self.allow_transient,
            )
        return self


@dataclass(frozen=True)
class _ServiceDefinition:
    origin: str
    service_cls: type[Any]
    key_template: str
    lifetime: ServiceLifetime
    eager: bool
    ctor: Ctor
    dtor: Dtor
    dependencies: dict[str, RequiredService]
    source: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None
    exposed_type: Any = None


@dataclass(frozen=True)
class _ResolvedDependency:
    param_name: str
    dep_key: str


@dataclass(frozen=True)
class _CompiledService:
    origin: str
    key: str
    lifetime: ServiceLifetime
    eager: bool
    ctor: Ctor
    dtor: Dtor
    signature: inspect.Signature
    static_kwargs: dict[str, Any]
    dependencies: tuple[_ResolvedDependency, ...]
    service_type: Any


class ServiceRegistry:
    _lock = threading.Lock()
    _definitions_by_origin: dict[str, _ServiceDefinition] = {}
    _definition_order: list[str] = []

    @classmethod
    def register(cls, definition: _ServiceDefinition) -> None:
        with cls._lock:
            existing = cls._definitions_by_origin.get(definition.origin)
            if existing is not None:
                return
            cls._definitions_by_origin[definition.origin] = definition
            cls._definition_order.append(definition.origin)

    @classmethod
    def definitions(cls) -> list[_ServiceDefinition]:
        with cls._lock:
            return [
                cls._definitions_by_origin[origin]
                for origin in cls._definition_order
                if origin in cls._definitions_by_origin
            ]

    @classmethod
    def clear_for_tests(cls) -> None:
        with cls._lock:
            cls._definitions_by_origin.clear()
            cls._definition_order.clear()


def require(target: str | type[Any], *, allow_transient: bool = False) -> RequiredService:
    if not isinstance(target, (str, type)):
        raise TypeError("require() expects a service key (str) or a service type")
    return RequiredService(target=target, allow_transient=allow_transient)


def _normalize_lifetime(lifetime: LifetimeLike) -> ServiceLifetime:
    if isinstance(lifetime, ServiceLifetime):
        return lifetime
    if isinstance(lifetime, int):
        try:
            return ServiceLifetime(lifetime)
        except ValueError as exc:
            raise ValueError(
                f"Unsupported lifetime value: {lifetime!r}. Use ServiceLifetime.SINGLETON/TRANSIENT or 0/1."
            ) from exc
    if isinstance(lifetime, str):
        normalized = lifetime.strip().lower()
        if normalized == "singleton":
            return ServiceLifetime.SINGLETON
        if normalized == "transient":
            return ServiceLifetime.TRANSIENT
        raise ValueError(
            f"Unsupported lifetime string: {lifetime!r}. Use 'Singleton' or 'Transient'."
        )
    raise TypeError(
        "Invalid lifetime type: "
        f"{type(lifetime)!r}. Use ServiceLifetime, int (0/1), or 'Singleton'/'Transient'."
    )


def _extract_ctors(service_cls: type[Any]) -> tuple[Ctor, Dtor]:
    lifespan_tasks = getattr(service_cls, "LifespanTasks", None)
    if lifespan_tasks is None:
        raise TypeError(f"{service_cls!r} has no LifespanTasks class")

    ctor = getattr(lifespan_tasks, "ctor", None)
    if ctor is None or not callable(ctor):
        raise TypeError(f"{service_cls!r}.LifespanTasks.ctor must be callable")

    dtor = getattr(lifespan_tasks, "dtor", None)
    if dtor is not None and not callable(dtor):
        raise TypeError(f"{service_cls!r}.LifespanTasks.dtor must be callable or None")

    return ctor, dtor


def _extract_dependencies(ctor: Ctor) -> dict[str, RequiredService]:
    deps: dict[str, RequiredService] = {}
    sig = inspect.signature(ctor)
    for name, parameter in sig.parameters.items():
        default = parameter.default
        if isinstance(default, RequiredService):
            deps[name] = default
    return deps


def _register_service_class(
    service_cls: type[Any],
    *,
    key: str,
    source: Mapping[str, Any] | Callable[[], Mapping[str, Any]] | None = None,
    lifetime: LifetimeLike = ServiceLifetime.SINGLETON,
    eager: bool = False,
    exposed_type: Any = None,
) -> type[Any]:
    ctor, dtor = _extract_ctors(service_cls)
    deps = _extract_dependencies(ctor)

    resolved_lifetime = _normalize_lifetime(lifetime)
    if eager and resolved_lifetime != ServiceLifetime.SINGLETON:
        raise ValueError(
            f"Service '{service_cls.__name__}' cannot be eager with transient lifetime."
        )
    definition = _ServiceDefinition(
        origin=f"{service_cls.__module__}.{service_cls.__qualname__}",
        service_cls=service_cls,
        key_template=key,
        lifetime=resolved_lifetime,
        eager=eager,
        ctor=ctor,
        dtor=dtor,
        dependencies=deps,
        source=source,
        exposed_type=exposed_type,
    )
    ServiceRegistry.register(definition)
    return service_cls


def Service(
    key: str,
    *,
    lifetime: LifetimeLike = ServiceLifetime.SINGLETON,
    eager: bool = False,
    exposed_type: Any = None,
) -> Callable[[type[Any]], type[Any]]:
    def _decorator(service_cls: type[Any]) -> type[Any]:
        return _register_service_class(
            service_cls,
            key=key,
            source=None,
            lifetime=lifetime,
            eager=eager,
            exposed_type=exposed_type,
        )

    return _decorator


def ServiceDict(
    key: str,
    *,
    dict: Mapping[str, Any] | Callable[[], Mapping[str, Any]],
    lifetime: LifetimeLike = ServiceLifetime.SINGLETON,
    eager: bool = False,
    exposed_type: Any = None,
) -> Callable[[type[Any]], type[Any]]:
    def _decorator(service_cls: type[Any]) -> type[Any]:
        return _register_service_class(
            service_cls,
            key=key,
            source=dict,
            lifetime=lifetime,
            eager=eager,
            exposed_type=exposed_type,
        )

    return _decorator


def import_service_modules(package_name: str = "app.services") -> None:
    package = importlib.import_module(package_name)
    package_path = getattr(package, "__path__", None)
    if package_path is None:
        return

    for module_info in pkgutil.walk_packages(package_path, prefix=f"{package.__name__}."):
        module_name = module_info.name
        parts = module_name.split(".")
        if any(part.startswith("_") for part in parts[2:]):
            continue
        importlib.import_module(module_name)


def _render_service_key(key_template: str, dict_key: str) -> str:
    if "{}" in key_template:
        return key_template.replace("{}", dict_key)
    return f"{dict_key}_{key_template}"


def _coerce_mapping_value(
    value: Any,
    *,
    signature: inspect.Signature,
    dependency_params: set[str],
) -> dict[str, Any]:
    if hasattr(value, "model_dump") and callable(value.model_dump):
        raw = value.model_dump()
        if not isinstance(raw, Mapping):
            raise TypeError("model_dump() must return a mapping")
        return dict(raw)

    if isinstance(value, Mapping):
        return dict(value)

    for name, parameter in signature.parameters.items():
        if name in dependency_params:
            continue
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            continue
        return {name: value}

    raise TypeError("Unable to map ServiceDict value to ctor parameters")


def _resolve_source(source: Mapping[str, Any] | Callable[[], Mapping[str, Any]]) -> Mapping[str, Any]:
    resolved = source() if callable(source) else source
    if not isinstance(resolved, Mapping):
        raise TypeError("ServiceDict source must resolve to a mapping")
    return resolved


def _expand_definitions() -> list[ExpandedDefinition]:
    expanded: list[ExpandedDefinition] = []

    for definition in ServiceRegistry.definitions():
        signature = inspect.signature(definition.ctor)
        dependency_params = set(definition.dependencies.keys())

        if definition.source is None:
            expanded.append((definition, definition.key_template, {}, definition.dependencies, signature))
            continue

        source_mapping = _resolve_source(definition.source)
        for raw_dict_key, raw_value in source_mapping.items():
            dict_key = str(raw_dict_key)
            service_key = _render_service_key(definition.key_template, dict_key)
            static_kwargs = _coerce_mapping_value(
                raw_value,
                signature=signature,
                dependency_params=dependency_params,
            )
            deps = {
                param_name: dep.render_for_dict_key(dict_key)
                for param_name, dep in definition.dependencies.items()
            }
            expanded.append((definition, service_key, static_kwargs, deps, signature))

    return expanded


def _resolve_dependency_targets(
    compiled: list[ExpandedDefinition],
) -> list[_CompiledService]:
    by_key: dict[str, ResolvedByKey] = {}
    type_index: dict[Any, list[str]] = defaultdict(list)

    for definition, key, static_kwargs, deps, signature in compiled:
        if key in by_key:
            first = by_key[key][0]
            raise RuntimeError(
                f"Duplicate service key '{key}' from {first.origin} and {definition.origin}."
            )

        service_type = definition.exposed_type
        if service_type is None:
            service_type = ServiceContainer._infer_service_type(definition.ctor)
        if service_type is None:
            service_type = definition.service_cls
        by_key[key] = (definition, deps, signature, static_kwargs, service_type)
        type_index[service_type].append(key)

    dependency_map: dict[str, set[str]] = {}
    resolved: dict[str, ResolvedSpec] = {}

    for key, (definition, deps, signature, static_kwargs, _service_type) in by_key.items():
        dep_keys: set[str] = set()
        resolved_deps: list[_ResolvedDependency] = []

        for param_name, dep in deps.items():
            if isinstance(dep.target, str):
                dep_key = dep.target
            else:
                candidate_keys = type_index.get(dep.target, [])
                if not candidate_keys:
                    raise RuntimeError(
                        f"Service '{key}' depends on type {dep.target!r}, but no service provides that type."
                    )
                if len(candidate_keys) > 1:
                    raise RuntimeError(
                        "Service "
                        f"'{key}' depends on type {dep.target!r}, "
                        f"but multiple services provide it: {candidate_keys}."
                    )
                dep_key = candidate_keys[0]

            if dep_key not in by_key:
                raise RuntimeError(
                    f"Service '{key}' depends on '{dep_key}', but that service is not registered."
                )

            target_definition = by_key[dep_key][0]
            if (
                definition.lifetime == ServiceLifetime.SINGLETON
                and target_definition.lifetime == ServiceLifetime.TRANSIENT
                and not dep.allow_transient
            ):
                raise RuntimeError(
                    f"Singleton service '{key}' depends on transient service '{dep_key}'. "
                    "Use require(..., allow_transient=True) only if this is intentional."
                )

            dep_keys.add(dep_key)
            resolved_deps.append(_ResolvedDependency(param_name=param_name, dep_key=dep_key))

        dependency_map[key] = dep_keys
        resolved[key] = (
            definition,
            signature,
            static_kwargs,
            tuple(resolved_deps),
            by_key[key][4],
        )

    registration_order = _topological_registration_order(dependency_map)

    result: list[_CompiledService] = []
    for key in registration_order:
        definition, signature, static_kwargs, resolved_deps, service_type = resolved[key]
        result.append(
            _CompiledService(
                origin=definition.origin,
                key=key,
                lifetime=definition.lifetime,
                eager=definition.eager,
                ctor=definition.ctor,
                dtor=definition.dtor,
                signature=signature,
                static_kwargs=static_kwargs,
                dependencies=resolved_deps,
                service_type=service_type,
            )
        )

    return result


def _topological_registration_order(dependency_map: dict[str, set[str]]) -> list[str]:
    indegree = dict.fromkeys(dependency_map, 0)
    outgoing: dict[str, set[str]] = defaultdict(set)

    for node, deps in dependency_map.items():
        indegree[node] = len(deps)
        for dep in deps:
            outgoing[dep].add(node)

    queue = deque(node for node, degree in indegree.items() if degree == 0)
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for successor in outgoing.get(node, set()):
            indegree[successor] -= 1
            if indegree[successor] == 0:
                queue.append(successor)

    if len(order) != len(dependency_map):
        cycle = _detect_cycle(dependency_map)
        if cycle:
            cycle_path = " -> ".join(cycle)
            raise RuntimeError(f"Detected circular service dependency: {cycle_path}")
        raise RuntimeError("Detected circular service dependency")

    return order


def _detect_cycle(dependency_map: dict[str, set[str]]) -> list[str] | None:
    UNVISITED = 0
    VISITING = 1
    VISITED = 2

    state: dict[str, int] = dict.fromkeys(dependency_map, UNVISITED)
    stack: list[str] = []

    def dfs(node: str) -> list[str] | None:
        state[node] = VISITING
        stack.append(node)

        for dep in dependency_map.get(node, set()):
            dep_state = state.get(dep, UNVISITED)
            if dep_state == UNVISITED:
                cycle = dfs(dep)
                if cycle is not None:
                    return cycle
            elif dep_state == VISITING:
                start = stack.index(dep)
                return stack[start:] + [dep]

        stack.pop()
        state[node] = VISITED
        return None

    for node in dependency_map:
        if state[node] == UNVISITED:
            cycle = dfs(node)
            if cycle is not None:
                return cycle

    return None


def build_service_plan() -> list[_CompiledService]:
    expanded = _expand_definitions()
    return _resolve_dependency_targets(expanded)


def _make_bound_ctor(container: ServiceContainer, spec: _CompiledService) -> Ctor:
    ctor = spec.ctor
    signature = spec.signature.replace(return_annotation=spec.service_type)

    async def _bound_ctor(*args: Any, **kwargs: Any) -> Any:
        provided_names: set[str] = set()
        try:
            provided_names = set(signature.bind_partial(*args, **kwargs).arguments)
        except TypeError:
            # Defer detailed signature errors to the original ctor call.
            provided_names = set()

        call_kwargs = dict(spec.static_kwargs)
        for name in provided_names:
            call_kwargs.pop(name, None)
        call_kwargs.update(kwargs)

        request = container.current_request()
        request_kwarg_name = container.request_kwarg_name()

        for dep in spec.dependencies:
            if dep.param_name in provided_names or dep.param_name in call_kwargs:
                continue
            dep_kwargs: dict[str, Any] = {}
            if request is not None:
                dep_kwargs[request_kwarg_name] = request
            call_kwargs[dep.param_name] = await container.aget_by_key(dep.dep_key, **dep_kwargs)

        if inspect.iscoroutinefunction(ctor):
            return await ctor(*args, **call_kwargs)
        if inspect.isasyncgenfunction(ctor):
            return ctor(*args, **call_kwargs)

        result = await asyncio.to_thread(ctor, *args, **call_kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    _bound_ctor.__name__ = f"autoreg_ctor_{spec.key}"
    _bound_ctor.__qualname__ = _bound_ctor.__name__
    _bound_ctor.__signature__ = signature
    annotations = dict(getattr(ctor, "__annotations__", {}))
    annotations["return"] = spec.service_type
    _bound_ctor.__annotations__ = annotations
    return _bound_ctor


async def register_services_from_registry(container: ServiceContainer) -> list[str]:
    plan = build_service_plan()
    for spec in plan:
        bound_ctor = _make_bound_ctor(container, spec)
        await container.register(
            spec.key,
            spec.lifetime,
            bound_ctor,
            spec.dtor,
        )
        if spec.eager:
            await container.aget_by_key(spec.key)

    logger.debug("[LIFESPAN] Auto-registered services", count=len(plan))
    return [spec.key for spec in plan]
