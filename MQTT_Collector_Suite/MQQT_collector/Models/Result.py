from sqlalchemy import Integer, Column, String, Text

from DatabaseBusiness.database_manager import Base


class Result(Base):

	STATUS_PROCESSING = "Processing"
	STATUS_COMPLETE = "Completed"

	__tablename__ = 'result'
	id = Column('id', Integer, primary_key=True, unique=True)
	status = (Column('status', String(32)))
	result = (Column('result', Text))


