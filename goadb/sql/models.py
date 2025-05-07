from dataclasses import dataclass

@dataclass
class DBConfig:
    username: str
    password: str
    host: str
    database: str
