# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, TEXT, UniqueConstraint, Index
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine
import re

engine = create_engine(
    "mysql+pymysql://root:root@localhost/mysql?charset=utf8", max_overflow=5)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class Newstable(Base):
    __tablename__ = 'learn_newstable'
    id = Column(Integer, primary_key=True)
    news_body = Column(TEXT)
    news_title = Column(String(32))
    news_thread = Column(String(32))
    news_url = Column(String(64))
    __table_args__ = (
        UniqueConstraint('id', 'news_title', name='uix_id_name'),
        Index('ix_id_name', 'news_title', 'news_thread', 'news_url'),
    )


Base.metadata.create_all(engine)
session.execute('alter table learn_newstable convert to character set gbk')


class News163Pipeline(object):
    def process_item(self, item, spider):
        return item


class News163InfoPipeline(object):
    def open_spider(self, spider):
        self.f = open('News163Info.txt', 'w')

    def close_spider(self, spider):
        self.f.close()

    def process_item(self, item, spider):
        try:
            line = str(dict(item)) + '\n'
            self.f.write(line)
            dic = dict(item)
            obc = Newstable(news_body=re.sub('[\[\]]', ' ', str(
                dic['news_body'])), news_title=dic['news_title'], news_thread=dic['news_thread'], news_url=dic['news_url'])
            session.add(obc)
            session.commit()
        except:
            pass
        return item
