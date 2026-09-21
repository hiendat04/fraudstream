"""Adds the app version to every response, so a rollout can be watched request by request."""


class VersionHeader:
    """A plain ASGI middleware, so background tasks keep running after the answer."""

    def __init__(self, app, version: str):
        self.app = app
        self.header = (b"x-app-version", version.encode())

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_with_version(message):
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), self.header]
            await send(message)

        await self.app(scope, receive, send_with_version)
