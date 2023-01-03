from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
import time
import os
from utils import take_screenshot, convert_dt_objects
from databases import insert_into_database


target = "psql"
table_name = "crawlers.navegantes"
columns_mapping = {
    "viagem_id": "str",
    "navio": "str",
    "comprimento_m": "float",
    "armador": "str",
    "viagem": "str",
    "servico": "str",
    "dt_deadline": "datetime",
    "dt_chegada": "datetime",
    "dt_atracadao": "datetime",
    "dt_previsao_saida": "datetime",
    "kind": "str",
}

class NAV():

    def exec_crawler(self, message, cfg):

        self.cfg = cfg

        print("NAV Crawler >> Initializing crawler...")
        self.init_crawler()

        print("NAV Crawler >> Loading ships...")
        # load ships
        ship_list = self.load_schedule_list('//*[@id="tabID"]')

        # # # # save unmoored ships into database
        print("NAV Crawler >> Saving ships on DB...")
        self.save(ship_list=ship_list)

        print("NAV Crawler >> Taking and Saving Screenshots...")
        self.take_screenshots('//*[@id="tabID"]')    

        # closing selenium window
        print("NAV Crawler >> Closing crawler...")
        self.exit_crawler()

        print("NAV Crawler >> Finished successfully...")
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
        nav.get(self.cfg.get('SiteSection', 'nav.url'))
        nav.maximize_window()
        
        time.sleep(3)
        self.navigator = nav    
    
    def exit_crawler(self):
        self.navigator.close()
        self.navigator.quit()     

    def load_schedule_list(self, xpath):
        main_tab = self.navigator.find_element_by_xpath(xpath)
        types_tab = main_tab.find_elements(By.TAG_NAME, 'li')
        ship_list = []
        for i in range(len(types_tab)):
            types_tab[i].click()
            time.sleep(0.4)
            lines = self.navigator.find_elements(By.TAG_NAME, 'table')[i].find_elements(By.TAG_NAME, 'tr')[1:]
            time.sleep(0.1)
            ship_list += [
                {
                    **dict(zip([k for k in columns_mapping], [value.text for value in line.find_elements(By.TAG_NAME, 'td')])),
                    "kind": types_tab[i].text
                }
                for line in lines
            ]
        return ship_list
        

    def save(self, ship_list):
        records = [
            convert_dt_objects(
                record, 
                columns_mapping, 
                '%d/%m/%Y %H:%M', 
                self.__class__.__name__.lower()
            )
            for record in ship_list
        ]

        insert_into_database(
            records, 
            target,
            table_name,
            columns_mapping,
            self.cfg
        )


    def take_screenshots(self, xpath):
        main_tab = self.navigator.find_element_by_xpath(xpath)
        types_tab = main_tab.find_elements(By.TAG_NAME, 'li')
        for i in range(len(types_tab)):
            types_tab[i].click()
            take_screenshot(
                    f"nav/print_{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.png",
                    self.navigator
            )
            lines = self.navigator.find_elements(By.TAG_NAME, 'table')[i].find_elements(By.TAG_NAME, 'tr')[1:]

            if len(lines) > 50:
                for i in range(int(len(lines)/50)+1):
                    idx = i * 250
                    prev_idx = idx - 250
                    print("Taking screenshot prev_idx:" + str(prev_idx) + " - idx: " + str(idx))
                    str_idx = "window.scrollBy("+str(prev_idx)+"," + str(idx) +")"
                    self.navigator.execute_script(str_idx)
                    take_screenshot(
                            f"nav/print_{datetime.today().strftime('%Y-%m-%d %H:%M:%S')}.png",
                            self.navigator
                    )

