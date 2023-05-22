import json
from typing import Dict, Tuple

RSE_KEY = 'dst-rse'
URL_KEY = 'dst-url'
HAS_COMPANION = 'has-companion'

class Message:

    def __init__(self, message):
        print(message.value())
        self._message = message

    def extract_rse_info(self) -> Tuple[str, str]:
        value = self._message.value()
        msg = json.loads(value)

        payload = msg['payload']

        dstRse = payload[RSE_KEY]
        dstUrl = payload[URL_KEY]
        has_companion = payload.get(HAS_COMPANION)
        return dstRse, dstUrl, has_companion
        
