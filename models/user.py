from pydantic import BaseModel, EmailStr

class UserSignupRequest(BaseModel):
    email: EmailStr
    password: str

class UserLoginRequest(BaseModel):
    email: EmailStr
    password: str

class UserTrackRequest(BaseModel):
    email: str
    full_name: str = ""
    provider: str = ""
    avatar_url: str = ""

