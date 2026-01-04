import os
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer()
async def validate_api_key(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> None:
    """
    Validate if the credential matches the predefined api keys.
    """

    if credentials.credentials != os.getenv("auth_api_key"):
        raise HTTPException(status_code=401, detail="Invalid API key")