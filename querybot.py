import openai
from pyspark.sql import SparkSession
from langchain_experimental.agents import create_spark_dataframe_agent
from langchain_community.llms import OpenAI

class QueryBot():
    def __init__(self, api_key):
        # OPENAI authentication
        self.OPENAI_API_KEY = api_key
        
    def initialize(self):
        openai.api_key = self.OPENAI_API_KEY
        

    def get_response(self, new_query):
        
        if new_query == '':
            return 'Query is empty'

        spark = SparkSession.builder.getOrCreate()
        df_leads = spark.read.csv('Mock_Eqp_Data_Updated.csv', mode="DROPMALFORMED",inferSchema=True, header = True)

        # langchain_agent = create_spark_dataframe_agent(llm=OpenAI(temperature=0), df=df_leads, verbose=True, return_intermediate_steps=True)

        # response = langchain_agent.run(new_query)
        # response = langchain_agent({"input":new_query})
        # response = str(df_leads.count())
        
        response = {}
        df_filtered = df_leads.select('EquipmentID', 'EquipmentDescription')
        # list_ib = map(lambda row: row.asDict(), df_filtered.collect())

        response['query'] = new_query
        response['result'] = str(df_filtered.count())
        
        return response