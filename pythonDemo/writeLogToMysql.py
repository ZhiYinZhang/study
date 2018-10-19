#!/usr/bin/env python
# -*- coding:utf-8 -*-
import logging
import pymysql
from sqlalchemy import (Column,DateTime,Integer,MetaData,String,Table,create_engine,text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper,sessionmaker

class LoggerHandlerToMysql(logging.Handler):
    def __init__(self,configdb_str,table_name):
        self.config_engine=create_engine(configdb_str)
        self.ConfigSession=sessionmaker(bind=self.config_engine)
        self.config_session=self.ConfigSession()
        metadata=MetaData(self.config_engine)
        log_table=Table(table_name,metadata,
                        Column('id',Integer,primary_key=True),
                        Column('create_time',DateTime,nullable=False,server_default=text("'0000-00-00 00:00:00.000'")),
                        Column('level_name',String(10)),
                        Column('message',String(255)),
                        Column('split_type',String(10)),
                        Column('exc_info',String(255)),
                        Column('exc_text',String(255)),
                        Column('file_name',String(100)),
                        Column('line_no',Integer),
                        Column('func_name',String(255)),
                        Column('stack_info',String(255))
                        )
        metadata.create_all(self.config_engine)
        self.LogModel=getModel(table_name,self.config_engine)
        logging.Handler.__init__(self)


    def emit(self, record):
        log_model=self.LogModel()
        log_model.create_time=str(record.asctime).replace(',','.')
        log_model.level_name=record.levelname
        message=record.message
        message=message.split("---")
        log_model.message=message[0].strip()
        if len(message)>1:
            log_model.split_type=message[1].strip()
        if len(message)>2:
            log_model.split_base=message[2].strip()
        log_model.exc_info=record.exc_info
        log_model.exc_text=record.exc_text
        log_model.file_name=record.filename
        log_model.line_no=record.lineno
        log_model.func_name=record.funcName
        log_model.stack_info=record.stack_info
        self.config_session.add(log_model)
        self.config_session.commit()
        pass
    def close(self):
        self.config_session.commit()
        self.config_session.close()
        pass
