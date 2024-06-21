from fastapi import WebSocket


class WebSocketHandler:
    websocket: WebSocket = None
    @staticmethod
    async def send_message(message: str):
        await WebSocketHandler.websocket.send_text(message)

    @staticmethod
    async def receive_message() -> str:
        return await WebSocketHandler.websocket.receive_text()
