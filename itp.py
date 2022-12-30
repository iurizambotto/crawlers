from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
# from google.cloud import storage
import time
import os
import mysql.connector
import base64
from utils import take_screenshot

columns_mapping = {
    'berco': "str",
    'navio': "str",
    'viagem': "str",
    'armador': "str",
    'servico': "str",
    'compr_m': "float",
    'largura_m': "float",
    'dt_abertura_do_gate': "datetime",
    'dt_deadline': "datetime",
    'dt_previsao_chegada': "datetime",
    'dt_chegada': "datetime",
    'dt_previsao_atracacao': "datetime",
    'dt_atracacao': "datetime",
    'dt_previsao_saida': "datetime",
    'dt_saida': "datetime",
}

class ITP():

    def exec_crawler(self, message, cfg):

        self.cfg = cfg

        print("ITP Crawler >> Initializing crawler...")
        self.init_crawler()

        print("ITP Crawler >> Loading ships...")
        # load ships
        ship_list = self.load_schedule_list('question')

        # # save unmoored ships into database
        print("ITP Crawler >> Saving ships on DB...")
        self.save(ship_list=ship_list)

        print("ITP Crawler >> Taking and Saving Screenshots...")
        self.take_screenshots('question')    

        # closing selenium window
        print("ITP Crawler >> Closing crawler...")
        self.exit_crawler()

        print("ITP Crawler >> Finished successfully...")
        message.ack()

    def init_crawler(self):

        path = self.cfg.get('ChromeDriverSection', 'chromedriver.path')

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument('--headless')
        chrome_options.add_argument("--window-size=3840,2160")
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.getcwd(), 'download.prompt_for_download': False})

        nav = webdriver.Chrome(
                os.path.join(path, 'chromedriver'),
                chrome_options=chrome_options)
        nav.get(self.cfg.get('SiteSection', 'itp.url'))
        nav.maximize_window()
        
        agree_btn = nav.find_element_by_xpath('//*[@id="cn-accept-cookie"]')
        agree_btn.click()

        time.sleep(3)
        self.navigator = nav    
    
    def exit_crawler(self):
        self.navigator.close()
        self.navigator.quit()

    def load_schedule_list(self, class_name):
        time.sleep(0.5)
        questions = self.navigator.find_elements_by_class_name(class_name)
        all_values = [
                    {
                            **dict(zip([k for k in columns_mapping], value)),
                            "kind": question.find_elements_by_class_name("title")[0].text
                    }
                for question in questions
                for value in self.get_table_by_question(question)
        ]
        entities = [
            ITPEntity(**value)
            for value in all_values
        ]

        return entities

    def save(self, ship_list):

        mydb = mysql.connector.connect(
          host=self.cfg.get('MySQLSection', 'db.host'),
          user=self.cfg.get('MySQLSection', 'db.user'),
          password=self.cfg.get('MySQLSection', 'db.password'),
          database=self.cfg.get('MySQLSection', 'db.name')
        )

        mycursor = mydb.cursor()
        for s in range(len(ship_list)):
            ship_list[s].convert_dt_objects('%Y-%m-%d %H:%M:%S')
            
            sql = "INSERT INTO crawlers.itp (kind, berco, navio, viagem, armador, servico, compr_m, largura_m, dt_abertura_do_gate, dt_deadline, dt_previsao_chegada, dt_chegada, dt_previsao_atracacao, dt_atracacao, dt_previsao_saida, dt_saida, createdat, updatedat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"

            val = (
                ship_list[s].kind,
                ship_list[s].berco,
                ship_list[s].navio,
                ship_list[s].viagem,
                ship_list[s].armador,
                ship_list[s].servico,
                ship_list[s].compr_m,
                ship_list[s].largura_m,
                ship_list[s].dt_abertura_do_gate_dtobj,
                ship_list[s].dt_deadline_dtobj,
                ship_list[s].dt_previsao_chegada_dtobj,
                ship_list[s].dt_chegada_dtobj,
                ship_list[s].dt_previsao_atracacao_dtobj,
                ship_list[s].dt_atracacao_dtobj,
                ship_list[s].dt_previsao_saida_dtobj,
                ship_list[s].dt_saida_dtobj,
            )
            mycursor.execute(sql, val)
            mydb.commit()

    def take_screenshots(self, class_name):
        questions = self.navigator.find_elements_by_class_name(class_name)
        time.sleep(0.5)
        self.navigator.execute_script("window.scrollBy(0, -1000)")
        for i in range(len(questions)):
                time.sleep(.5)
                questions[i].click()
                len_records = len(questions[i].find_elements(By.TAG_NAME, 'tr'))
                if i == 0:
                    self.navigator.execute_script("window.scrollBy(0, 650)")

                time.sleep(.5)
                take_screenshot(
                        f"data/itp/print_{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.png",
                        self.navigator
                )
                for j in range(0, len_records, 7):
                        if j > 8:
                                function = f"""
                                var elements = document.getElementsByClassName("question")[{i}].getElementsByTagName("tr");
                                elements[{j-8}].scrollIntoView();
                                """
                                self.navigator.execute_script(function)
                                time.sleep(0.5)
                                take_screenshot(
                                        f"data/itp/print_{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.png",
                                        self.navigator
                                )

    def get_table_by_question(self, question):
            question.click()
            lines = question.find_elements(By.TAG_NAME, 'tr')
            values = [
                    [
                            value.text
                            for value in line.find_elements(By.TAG_NAME, 'td')
                    ]
                    for line in lines
            ][1:]
            question.click()
            return values

class ITPEntity():
    def __init__(self, berco, navio, viagem, armador, servico, compr_m, largura_m, kind, dt_abertura_do_gate, dt_deadline, dt_previsao_chegada, dt_previsao_atracacao, dt_previsao_saida, dt_chegada=None, dt_atracacao=None, dt_saida=None):
        self.berco = berco
        self.navio = navio
        self.viagem = viagem
        self.armador = armador
        self.servico = servico
        self.compr_m = compr_m
        self.largura_m = largura_m
        self.dt_abertura_do_gate = dt_abertura_do_gate
        self.dt_deadline = dt_deadline
        self.dt_previsao_chegada = dt_previsao_chegada
        self.dt_chegada = dt_chegada
        self.dt_previsao_atracacao = dt_previsao_atracacao
        self.dt_atracacao = dt_atracacao
        self.dt_previsao_saida = dt_previsao_saida
        self.dt_saida = dt_saida
        self.kind = kind


    def convert_dt_objects(self, str_pattern):
        for column, column_type in columns_mapping.items():
            if column_type == "datetime":
                if getattr(self, column) != "":
                    setattr(
                        self,
                        f"{column}_dtobj",
                        datetime.strptime(
                            getattr(self, column), str_pattern
                        )
                    )
                else:
                    setattr(self, f"{column}_dtobj", None)
