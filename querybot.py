import openai
from pyspark.sql import SparkSession
from langchain_experimental.agents import create_spark_dataframe_agent
from langchain_openai import OpenAI
from flask import jsonify

AGENT_SYSTEM_MESSAGE = """
You are working with a spark dataframe in Python. The name of the dataframe is 'df.
You should use the tools below to answer the question posed of you:

python_repl_ast: A Python shell. Use this to execute python commands. Input should be a valid python command. When using this tool, 
sometimes output is abbreviated - make sure it does not look abbreviated before using it in your answer.

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [python_repl_ast]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat upto 5 times)
Thought: I now know the final answer
Final Answer: the Action Input from the intermediate steps


This is the result of `print(df.first())`: {df}

Begin!
Question: {input}
{agent_scratchpad}.

Follow the ##instructions## below while answering user query:
1) You have to return the filter query from last Action Input as your final output. You don't have to print anything else.
1) Whenever user asks questions, create a python filter query according to user criteria. 
2) Analyse the user question, and make sure you consider correct column names while answering. 
3) The rows in the df are equipment data
4) You must take into consideration the following ##examples##:

Example1:
Question: Give me equipments installed in Australia?"
Action Input: df.filter(df.Country == 'AU'))

5) when dealing with equipment contract related queries, ensure to use columns Active_ContractStartDate, Active_ContractEndDate, Latest_ContractStartDate, Latest_ContractEndDate to filter 

"""

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

        langchain_agent = create_spark_dataframe_agent(llm=OpenAI(temperature=0), df=df_leads, verbose=True, return_intermediate_steps=True, handle_parsing_errors=True, max_iterations=5, early_stopping_method="generate")
        
        langchain_agent.agent.llm_chain.prompt.template = AGENT_SYSTEM_MESSAGE

        # agent_response = langchain_agent.run(new_query)
        agent_response = langchain_agent({"input":new_query})
        
        # df_filtered = df_leads.select('EquipmentID', 'EquipmentDescription').distinct().limit(5)
        # list_ib = [row.asDict() for row in df_filtered.collect()]
        
        return agent_response['output']