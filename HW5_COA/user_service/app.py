import os
import jwt
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from passlib.context import CryptContext
from kafka import KafkaProducer
import json
# at the top of your file, replace the straight import with:
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from fastapi import FastAPI
app = FastAPI()
# remove any top‐level "producer = KafkaProducer(...)" and instead:

def make_producer():
    """Try until Kafka comes up."""
    retries = 0
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=["kafka:9092"],
                value_serializer=lambda v: json.dumps(v).encode(),
            )
        except NoBrokersAvailable:
            retries += 1
            if retries > 30:
                raise RuntimeError("Kafka broker never came up")
            print("Kafka not ready, retrying in 2s…")
            time.sleep(2)

producer = make_producer()
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

app = FastAPI(title="User Service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db_users:5432/usersdb")
JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-key")
JWT_ALGORITHM = "HS256"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    login = Column(String(80), unique=True, nullable=False)
    password_hash = Column(String(128), nullable=False)
    email = Column(String(120), unique=True, nullable=False)
    first_name = Column(String(80))
    last_name = Column(String(80))
    date_of_birth = Column(DateTime)
    phone = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class UserRegister(BaseModel):
    login: str
    password: str
    email: EmailStr
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    date_of_birth: Optional[datetime] = None
    phone: Optional[str] = None

class UserLogin(BaseModel):
    login: str
    password: str

@app.post("/users/register", status_code=201)
def register_user(user_data: UserRegister, db: Session = Depends(get_db)):
    existing = db.query(User).filter(
        (User.login == user_data.login) | (User.email == user_data.email)
    ).first()
    if existing:
        raise HTTPException(status_code=400, detail="User with this login or email already exists")

    hashed_pw = get_password_hash(user_data.password)
    new_user = User(
        login=user_data.login,
        password_hash=hashed_pw,
        email=user_data.email,
        first_name=user_data.first_name,
        last_name=user_data.last_name,
        date_of_birth=user_data.date_of_birth,
        phone=user_data.phone
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return {"message": "User registered successfully"}

def create_jwt_token(user_id: int) -> str:
    expire = datetime.utcnow() + timedelta(minutes=60)
    payload = {
        "sub": str(user_id),
        "exp": expire
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

@app.post("/users/login")
def login(user_data: UserLogin, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.login == user_data.login).first()
    if not user or not verify_password(user_data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid login or password")

    token = create_jwt_token(user_id=user.id)
    return {"access_token": token}

@app.get("/users/profile")
def get_profile(db: Session = Depends(get_db), user_id: int = 1):
    # Заглушка - в реальности проверять JWT
    user = db.query(User).get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "login": user.login,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "date_of_birth": user.date_of_birth,
        "phone": user.phone,
        "created_at": user.created_at,
        "updated_at": user.updated_at
    }

@app.put("/users/profile")
def update_profile(db: Session = Depends(get_db), user_id: int = 1, user_data: UserRegister = Depends()):
    # Заглушка - в реальности проверять JWT, нельзя менять login/password
    user = db.query(User).get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.first_name = user_data.first_name or user.first_name
    user.last_name = user_data.last_name or user.last_name
    user.date_of_birth = user_data.date_of_birth or user.date_of_birth
    user.phone = user_data.phone or user.phone
    user.email = user_data.email or user.email

    db.commit()
    db.refresh(user)
    return {"message": "Profile updated successfully"}

@app.post('/users/register')
def register(user: UserRegister):
    # …создание пользователя…
    producer.send('user-registrations', {
        'userId': str(created.id),
        'registeredAt': datetime.utcnow().isoformat()
    })
    return created

@app.get("/")
def root():
    return {"message": "User Service is running"}