from typing import Any

import requests
from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from functools import lru_cache

from ..dto.dto import UserDto

AUTHENTIK_ISSUER = "https://auth.mooslechner.dev/application/o/tech-atlas/"
AUTHENTIK_AUDIENCE = "NjrD7i3pcLYKUPRlzdCpqLJGlbUwCq2DPY9ceeIh"
JWKS_URL = f"{AUTHENTIK_ISSUER}jwks/"


bearer_scheme = HTTPBearer()


class AuthUser:
    def __init__(self, token: dict[str, Any]):
        self.token = token
        self.id = token["sub"]
        self.name = token["preferred_username"]
        self.email = token["email"]

    def serialize(self) -> UserDto:
        return UserDto(self.id, self.name, self.email)


@lru_cache(maxsize=1)
def get_jwks():
    response = requests.get(JWKS_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def verify_token(token: str):
    try:
        jwks = get_jwks()

        header = jwt.get_unverified_header(token)
        kid = header.get("kid")

        if not kid:
            raise HTTPException(401, "Missing kid in token header")

        jwk_dict = next(
            (key for key in jwks["keys"] if key["kid"] == kid),
            None,
        )

        if not jwk_dict:
            raise HTTPException(401, "Signing key not found")

        signing_key = jwt.PyJWK.from_dict(jwk_dict)

        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=AUTHENTIK_ISSUER,
            audience=AUTHENTIK_AUDIENCE,
        )

        return payload

    except jwt.PyJWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}",
        )


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)) -> AuthUser:
    return AuthUser(verify_token(credentials.credentials))


def require_any_entitlements(*required_entitlements):
    def checker(user=Depends(get_current_user)):
        entitlements = set(user.token.get("entitlements", []))

        if not entitlements.intersection(required_entitlements):
            raise HTTPException(403, "Insufficient permissions")

        return user.serialize()

    return checker


def require_all_entitlements(*required_entitlements):
    def checker(user=Depends(get_current_user)):
        entitlements = set(user.token.get("entitlements", []))

        if len(entitlements.intersection(required_entitlements)) != len(required_entitlements):
            raise HTTPException(403, "Insufficient permissions")

        return user.serialize()

    return checker
