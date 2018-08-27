
# coding: utf-8

# In[ ]:


import cPickle 
from time import strftime
import MySQLdb

class sql4cluster():
    def __init__(self, model_name, user="", password="", db="", host=""):
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.model_name = model_name
        
        if host is not "":
            self.mySQL_connect()

    def mySQL_connect(self):
        print('=====================================================')
        print('======== Connect to the remote mySQL server ========')
        print('=====================================================')
        
        print('Time : {}\n'.format(strftime('%Y-%m-%d_%H_%M')))
        self.db = MySQLdb.connect(host=self.host, port=3306, user=self.user, passwd=self.password, db=self.db,
                        charset="utf8")

        self.db.ping(True)
        self.cursor = self.db.cursor()

    def init_table(self):
        create_table = "Create table cluster_model1(ID int NOT NULL AUTO_INCREMENT, model_name VARCHAR(40), model BLOB, PRIMARY KEY (ID));"
        self.cursor.execute(create_table)
        self.db.commit()

    def insert_model_blob(self, trained_model):
        model_blob = cPickle.dumps(trained_model)
        add_blob = "INSERT INTO cluster_model1 (model_name, model) VALUES (%s, %s)"
        self.cursor.execute(add_blob, (self.model_name, model_blob))
        self.db.commit()

    def read_model_blob(self):
        print('Fetch the target model...')
        sql_cmd = "SELECT data FROM cluster_model1 WHERE model_name = %s"
        self.cursor.execute(sql_cmd, (self.model_name,))
        model_blob = self.cursor.fetchone()
        return model_blob

    def load_model_from_sql(self):
        self.mySQL_connect()
        sql_model = self.read_model_blob()
        model = cPickle.loads(sql_model[0])
        return model

    def disconnect(self):
        self.db.close()
        print('=====================================================')
        print('============ Close the remote connection ============')
        print('=====================================================')

    def save2sql(self, model):
        self.insert_model_blob(model)
    
    def load_model_from_pkl(self):
        model=None
        with open(self.model_name, 'rb') as file:  
            model = cPickle.load(file)
        return model

    def save_model_to_pkl(self, trained_model):
        with open(self.model_name, 'wb') as file:  
            cPickle.dump(trained_model, file)
        model_blob = cPickle.dumps(trained_model)


