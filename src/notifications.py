import sqlalchemy
from sqlalchemy_serializer import SerializerMixin
from src.db_session import SqlAlchemyBase


class Notification(SqlAlchemyBase, SerializerMixin):
    __tablename__ = 'notification'
    __table_args__ = {'extend_existing': True}
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    chat_id = sqlalchemy.Column(sqlalchemy.Integer)
    condition = sqlalchemy.Column(sqlalchemy.String)
