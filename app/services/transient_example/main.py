from __future__ import annotations

from app.services import BaseService, Service


# Class name should have postfix "ServiceT" if the service is a transient service
@Service("example_transient", lifetime="transient")
class ExampleServiceT(BaseService):
    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor() -> ExampleServiceT:
            return ExampleServiceT()

    hello_msg = "Hello!"

    async def hello(self):
        return self.hello_msg
