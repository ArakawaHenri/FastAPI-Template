from __future__ import annotations

from app.services import BaseService


# Class name should have postfix "ServiceT" if the service is a transient service
class ExampleServiceT(BaseService):
    class LifespanTasks(BaseService.LifespanTasks):
        @staticmethod
        async def ctor() -> ExampleServiceT:
            return ExampleServiceT()

        dtor = None

    hello_msg = "Hello!"

    async def hello(self):
        return self.hello_msg
