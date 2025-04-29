#!/usr/bin/env python
from random import randint
from typing import Any, Dict, List
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.chains.summarize import load_summarize_chain
# from langchain_ollama import ChatOllama
from langchain.llms import Ollama
from crewai.agent import Agent
import json
from pydantic import BaseModel
from langchain_core.prompts import ChatPromptTemplate
from crewai.flow import Flow, listen, start
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta, date
from marketingflow.crews.poem_crew.poem_crew import PoemCrew
from marketingflow.crews.poem_crew.pitch_crew import PitchCrew
import uuid
import random

class flowstate(BaseModel):
    docs: Any = ''
    chunks: Any = ''
    result: Any = ''
    searches: List[Any] = []
    params: Dict[Any, Any] = {}
    pitch:Any = ''
    product_params: Any = {}

# class jsonresponse(BaseModel):



class PoemFlow(Flow[flowstate]):

    @start()
    def load_pdf(self):
        print("loading the document")
        pdf_path = r'C:\Users\MeesalaMrYeswanthKum\Desktop\GenAI\marketing\marketingflow\src\marketingflow\gold_protein.pdf'
        loader = PyPDFLoader(pdf_path)
        docs = loader.load()
        self.state.docs = docs
        # print(self.state.docs[0])
    
    @listen(load_pdf)
    def generate_chunks(self):
        print("--------------------------------------------------")
        print("generating chunks")
        splitter = RecursiveCharacterTextSplitter(chunk_size=1000,chunk_overlap=150)
        chunks = splitter.split_documents(self.state.docs)
        self.state.chunks = chunks

    @listen(generate_chunks)
    def summartize(self):
        print("generating summary")
        print("=======================================")
        llm = Ollama(model='llama3.1')
        # print(llm)
        # 
        chain = load_summarize_chain(llm=llm,chain_type='refine',verbose=True)
        # res = lcel.invoke({'input':self.state.chunks})
        result = chain.run(self.state.chunks)
        self.state.result = result
        print("======================================================================================================")
        print(result)
        print("======================================================================================================")
        
        
    @listen(summartize)
    def generate_parameters(self):
        print("Generating Summary")
        result = (
            PoemCrew()
            .crew()
            .kickoff(inputs={"topic": self.state.result})
        )

        print("parameters generated", result.raw)
        self.state.params = result.raw
        
    def search_allergen(self, param):
        if param:
            if "soy" in param.lower():
                return "soy"
    
        
    @listen(generate_parameters)
    def communicate_with_sql(self):
        print("retrieving json info")
        try:
            with open('response.json','r',encoding='utf-8') as file:
                print("accessed the file")
                product_data = json.load(file)
                print(product_data)
        
            conn = mysql.connector.connect(
                host='localhost',
                user='yash',
                password='Lucym123@', 
                database='pitch'      
            )

            if conn.is_connected():
                print("✅ Successfully connected to MySQL!")

                cursor = conn.cursor()

                # Print MySQL version
                cursor.execute("SELECT VERSION();")
                version = cursor.fetchone()
                print("🧠 MySQL version:", version)
                allergen = self.search_allergen(product_data["allergen_info"])
                flavour = product_data["flavour_options"].split(' ')[0]
            
                # Insert employee
                cursor.execute("select * from marketing_pitch where allergen_info like %s or flavour_options like %s", (f'%{allergen}%', f'%{flavour}%'))
                retreived = cursor.fetchall()
                ids = [i[0] for i in cursor.description]
                vals = [dict(zip(ids, row)) for row in retreived]
                with open('existing_records.json','w',encoding='utf-8') as file:
                    json.dump(vals, file)

                # take random record to create marketing pitch
                with open('existing_records.json','r',encoding='utf-8') as file:
                    ex_vals = json.load(file)
                self.state.product_params = random.choice(ex_vals)
                # print(retreived)
                records = cursor.lastrowid  # Get generated emp_no
                print(records)
                # Commit to DB
                conn.commit()
                print("✅ Parameters retrieved successfully.")

        except Error as e:
            print("❌ Error:", e)

        finally:
            # Cleanup
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
                print("🔒 MySQL connection closed.")

    
    @listen(communicate_with_sql)
    def pitch_creator(self):
        print("Generating marketing pitch")
        if self.state.product_params:
            pitch_res = (
                PitchCrew()
                .crew()
                .kickoff(inputs={"params": self.state.product_params})
            )
        # else:
        #     pitch_res = (
        #         PitchCrew()
        #         .crew()
        #         .kickoff(inputs={"params": self.state.params})
        #     )

        print("pitch generated", pitch_res.raw)
        self.state.pitch = pitch_res.raw




def kickoff():
    poem_flow = PoemFlow()
    poem_flow.kickoff()


def plot():
    poem_flow = PoemFlow()
    poem_flow.plot()


if __name__ == "__main__":
    kickoff()
