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

class flowstate(BaseModel):
    docs: Any = ''
    chunks: Any = ''
    result: Any = ''
    searches: List[Any] = []
    params: Dict[Any, Any] = {}
    pitch:Any = ''

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
        
        # crew_result = (
        #     PoemCrew()
        #     .crew()
        #     .kickoff(inputs={'topic':'what are the competitor brands for Optimum nutrition gold premium whey protein powder'})
        # )
        # print("********************************************************************************************************")
        # print(crew_result)
        # print("********************************************************************************************************")
            # print("*******************************************************")
            # print("Poem generated", result.raw)
            # print("*******************************************************")
            # self.state.searches.append(result.raw)

    @listen(summartize)
    def generate_parameters(self):
        print("Generating poem")
        result = (
            PoemCrew()
            .crew()
            .kickoff(inputs={"topic": self.state.result})
        )

        print("parameters generated", result.raw)
        self.state.params = result.raw
        # self.state.poem = result.raw
        
        
    @listen(generate_parameters)
    def communicate_with_sql(self):
        print("retrieving json info")
        try:
            with open('response.json','r',encoding='utf-8') as file:
                print("accessed the file")
                product_data = json.load(file)
                print(product_data)
        # except Error as e:
        #     print(e)
        

            
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

            
                add_params = (
                    "INSERT INTO marketing_pitch "
                    "(id,protein_per_serving, type_of_protein, BCAA_content, sugar_content, calories_per_serving," \
                    "flavour_options, mixability_texture, certifications, allergen_info, source_of_whey, packaging_info," \
                    "brand_reputation) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                )

                data_params = ('101',
                    product_data['protein_per_serving'],
                                 product_data['type_of_protein'],
                                 product_data['BCAA_content'],
                                 product_data['sugar_content'],
                                 product_data['calories_per_serving'],
                                 product_data['flavour_options'],
                                 product_data['mixability/texture'],
                                 product_data['certifications'],
                                 product_data['allergen_info'],
                                 product_data['source_of_whey'],
                                 product_data['packaging_info'],
                                 product_data['brand_reputation'])

                # Insert employee
                cursor.execute(add_params, data_params)
                emp_no = cursor.lastrowid  # Get generated emp_no

                # Commit to DB
                conn.commit()
                print("✅ Parameters inserted successfully.")

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
        pitch_res = (
            PitchCrew()
            .crew()
            .kickoff(inputs={"params": self.state.params})
        )

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
