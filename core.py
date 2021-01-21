import psycopg2
import redis

import datetime
import pytz
from notes import DogNotes,DelDog
from users import DogUser

class DogR(object):
    """
    使用redis来数据库缓存查询
    """
    def __init__(self, dogdb,dogr):
        """
        初始化
        """
        self.dogdb=dogdb
        self.r=dogr
    
    def get_info(self, dogid):
        """
        docstring
        """
        doginfo=self.r.hget('dogs',dogid)
        if doginfo is not None:
            # print('命中缓存:{}-{}'.format(dogid,doginfo))
            if doginfo=='True':
                return True
            else:
                return False
        else:
            udog=DogUser(self.dogdb,dogid)
            # print('未命中缓存:{}'.format(dogid))
            if udog.IsLocalDog or udog.IsVipDog:
                self.r.hset('dogs',dogid,'True')
                return True
            else:
                self.r.hset('dogs',dogid,'False')
                return False
    pass


def dogclean(dogdbi,dogri,dogs,doge):
    r = redis.Redis(host=dogri[0], port=dogri[1], db=dogri[3],password=dogri[2], decode_responses=True)
    pgdog = psycopg2.connect(database=dogdbi[2], user=dogdbi[3],password=dogdbi[4], host=dogdbi[0], port=dogdbi[1])
    stdog=datetime.datetime.strptime(dogs,'%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
    endog=datetime.datetime.strptime(doge,'%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
    idog = DogNotes(pgdog)
    sdf = idog.get_dognotes_list(stdog, endog)
    for sdi in sdf:
        if not idog.is_dognote_pin(sdi):
            r.sadd('doglist',sdi)
    dogn = r.srandmember('doglist')
    Dogr=DogR(pgdog,r)
    while dogn is not None:
        sdfp = str(dogn)
        sli = idog.get_alldog_notes([sdfp])
        dogf = False
        dog_id_lib = []
        dog_file_id = []
        dog_users_id = []
        for dogin, dogc in sli.items():
            dog_id_lib.append(dogin)
            dog_users_id.append(dogc[0])
            dog_file_id = dog_file_id+dogc[6]
            if dogc[7] or dogc[8]:
                dogf = dogf or True
            if dogc[5]>endog:
                dogf = dogf or True
                # print('超时:{}'.format(dogc))
                pass
        for udog in dog_users_id:
            dog_info=Dogr.get_info(udog)
            dogf = dogf or dog_info
        if not dogf:
            for sse in dog_id_lib:
                r.sadd('doglist2', sse)
                pass
            for ffd in dog_file_id:
                r.sadd('dogfile',ffd)
                pass
            pass
        for dogi in dog_id_lib:
            r.srem('doglist', dogi)
            pass
        dogn = r.srandmember('doglist')
    deldog=DelDog(pgdog)
    # sdfg=r.smembers('doglist2')
    dogn2 = r.srandmember('doglist2')
    dog01=0
    dog02=0
    while dogn2 is not None:
        dog01=dog01+1
        deldog.del_dog_note(dogn2)
        r.srem('doglist2',dogn2)
        print('已移除帖子 {}'.format(dogn2))
        dogn2 = r.srandmember('doglist2')

    dogn3= r.srandmember('dogfile')
    while dogn3 is not None:
        dog02=dog02+1
        deldog.del_dog_note(dogn3)
        r.srem('dogfile',dogn3)
        print('已移除文件 {}'.format(dogn3))
        dogn2 = r.srandmember('dogfile')
    print('共移除{}帖子 {}文件')
    # print(sdfg)




    pass