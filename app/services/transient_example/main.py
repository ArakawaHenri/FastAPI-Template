from __future__ import annotations

from app.services import BaseService, Service


# Class name should have postfix "ServiceT" if the service is a transient service
@Service("example_transient", lifetime="transient")
class ExampleServiceT(BaseService):
    @classmethod
    async def create(cls) -> ExampleServiceT:
        return cls()

    hello_msg = "Hello!"

    async def hello(self):
        return self.hello_msg
