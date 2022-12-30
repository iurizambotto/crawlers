from selenium import webdriver
from datetime import datetime
import time
import os
import mysql.connector
from utils import take_screenshot

columns_mapping = {
    "situacao": "str",
    "navio": "str",
    "viagem_tcp": "str",
    "service": "str",
    "dt_chegada_estimada": "datetime",
    "dt_chegada_barra": "datetime",
    "dt_previsao_atracacao": "datetime",
    "dt_previsao_saida": "datetime",
    "dt_atracacao": "datetime",
    "dt_saida": "datetime",
    "dt_deadline": "datetime",
    "dt_bws_starting": "datetime",
}

class TCP():

    def exec_crawler(self, message, cfg):

        self.cfg = cfg

        print("TCP Crawler >> Initializing crawler...")
        self.init_crawler()

        print("TCP Crawler >> Loading ships...")
        # load ships
        ship_list = self.load_schedule_list('mat-row')

        # # save unmoored ships into database
        print("TCP Crawler >> Saving ships on DB...")
        self.save(ship_list=ship_list)

        print("TCP Crawler >> Taking and Saving Screenshots...")
        self.take_screenshots()    

        # closing selenium window
        print("TCP Crawler >> Closing crawler...")
        self.exit_crawler()

        print("TCP Crawler >> Finished successfully...")
        message.ack()

    def init_crawler(self):

        path = self.cfg.get('ChromeDriverSection', 'chromedriver.path')

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument("--window-size=3840,2160")
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.getcwd(), 'download.prompt_for_download': False})

        nav = webdriver.Chrome(
                os.path.join(path, 'chromedriver'),
                chrome_options=chrome_options)
        nav.get(self.cfg.get('SiteSection', 'TCP.url'))
        nav.maximize_window()
        
        agree_btn = nav.find_element_by_xpath('/html/body/div[1]/div/a')
        agree_btn.click()

        time.sleep(3)
        self.navigator = nav    
    
    def exit_crawler(self):
        self.navigator.close()
        self.navigator.quit()     

    def load_schedule_list(self, xpath):

        ships = self.navigator.find_elements_by_class_name(xpath)
        ship_list = []    
        ship_count = 0
        for n in range(len(ships)):
            ship_elem = ships[n]
            if ship_elem.text != "":

                ship_count += 1
                print("--> Loading ship " + str(ship_count) + " of " + str(len(ships)))
                ship = {
                    "situacao": ships[n].find_elements_by_class_name("cdk-column-Situacao")[0].text,
                    "navio": ships[n].find_elements_by_class_name("cdk-column-Navio")[0].text,
                    "viagem_tcp": ships[n].find_elements_by_class_name("cdk-column-ViagemTcp")[0].text,
                    "service": ships[n].find_elements_by_class_name("cdk-column-Service")[0].text,
                    "dt_chegada_estimada": ships[n].find_elements_by_class_name("cdk-column-ChegadaEstimada")[0].text,
                    "dt_chegada_barra": ships[n].find_elements_by_class_name("cdk-column-ChegadaBarra")[0].text,
                    "dt_previsao_atracacao": ships[n].find_elements_by_class_name("cdk-column-PrevisaoAtracacao")[0].text,
                    "dt_previsao_saida": ships[n].find_elements_by_class_name("cdk-column-PrevisaoSaida")[0].text,
                    "dt_atracacao": ships[n].find_elements_by_class_name("cdk-column-Atracacao")[0].text,
                    "dt_saida": ships[n].find_elements_by_class_name("cdk-column-Saida")[0].text,
                    "dt_deadline": ships[n].find_elements_by_class_name("cdk-column-DeadLine")[0].text,
                    "dt_bws_starting": ships[n].find_elements_by_class_name("cdk-column-BWStarting")[0].text,
                }
                tcp_entity = TCPEntity(
                    **ship
                )

                ship_list.append(tcp_entity)                    

        return ship_list

    def save(self, ship_list):

        mydb = mysql.connector.connect(
          host=self.cfg.get('MySQLSection', 'db.host'),
          user=self.cfg.get('MySQLSection', 'db.user'),
          password=self.cfg.get('MySQLSection', 'db.password'),
          database=self.cfg.get('MySQLSection', 'db.name')
        )

        mycursor = mydb.cursor()

        for s in range(len(ship_list)):
            
            ship_list[s].convert_dt_objects('%d/%m/%Y %H:%M')
            
            sql = "INSERT INTO crawlers.tcp (situacao, navio, viagem_tcp, service, dt_chegada_estimada, dt_previsao_atracacao, dt_previsao_saida, dt_deadline, dt_bws_starting, dt_chegada_barra, dt_atracacao, dt_saida, createdat, updatedat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"

            val = (
                ship_list[s].situacao,
                ship_list[s].navio,
                ship_list[s].viagem_tcp,
                ship_list[s].service,
                ship_list[s].dt_chegada_estimada_dtobj,
                ship_list[s].dt_previsao_atracacao_dtobj,
                ship_list[s].dt_previsao_saida_dtobj,
                ship_list[s].dt_deadline_dtobj,
                ship_list[s].dt_bws_starting_dtobj,
                ship_list[s].dt_chegada_barra_dtobj,
                ship_list[s].dt_atracacao_dtobj,
                ship_list[s].dt_saida_dtobj
            )
            mycursor.execute(sql, val)
            mydb.commit()

    def take_screenshots(self):
        function = f"""
        var elements = document.getElementsByClassName("mat-header-row");
        elements[0].scrollIntoView();
        """
        self.navigator.execute_script(function)

        take_screenshot(
            f"tcp/print_{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.png",
            self.navigator
        )

        elements = self.navigator.find_elements_by_class_name("mat-row")

        for i in range(15, len(elements), 15):
                function = f"""
                var elements = document.getElementsByClassName("mat-row");
                elements[{i}].scrollIntoView();
                """
                self.navigator.execute_script(function)
                take_screenshot(
                    f"tcp/print_{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.png",
                    self.navigator
                )


class TCPEntity():
    def __init__(self, situacao, navio, viagem_tcp, service, dt_chegada_estimada, dt_previsao_atracacao, dt_previsao_saida, dt_deadline, dt_bws_starting, dt_chegada_barra=None, dt_atracacao=None, dt_saida=None):
        self.situacao = situacao
        self.navio = navio
        self.viagem_tcp = viagem_tcp
        self.service = service
        self.dt_chegada_estimada = dt_chegada_estimada
        self.dt_previsao_atracacao = dt_previsao_atracacao
        self.dt_previsao_saida = dt_previsao_saida
        self.dt_deadline = dt_deadline
        self.dt_bws_starting = dt_bws_starting
        self.dt_chegada_barra = dt_chegada_barra
        self.dt_atracacao = dt_atracacao
        self.dt_saida = dt_saida

    def convert_dt_objects(self, str_pattern):
        for column, column_type in columns_mapping.items():
            if column_type == "datetime":
                if getattr(self, column) != "" and "-" not in getattr(self, column):
                    setattr(
                        self,
                        f"{column}_dtobj",
                        datetime.strptime(
                            getattr(self, column), str_pattern
                        )
                    )
                else:
                    setattr(self, f"{column}_dtobj", None)
