import requests
import pandas as pd
import time
from tqdm import tqdm
from bs4 import BeautifulSoup
import cloudscraper
from datetime import datetime, date
import re
import json
import gdrive
import pytz
import joblib
import numpy as np

#################################
#SETUP PART
#################################

folder_id_json = "1GbM7j6NwoItCRPDoCHxLBv2ld2pYnnUj"
file_name_arm = "ArmEng_complete1.json"
file_name_aze = "AzeEng_complete1.json"

headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"}

scrape_id = "10uGwqArcMNM7sYtLKDKkQT8kKkO_6Ry1"

russian_months = {
    "январь": "January", "февраль": "February", "март": "March",
    "апрель": "April", "май": "May", "июнь": "June",
    "июль": "July", "август": "August", "сентябрь": "September",
    "октябрь": "October", "ноябрь": "November", "декабрь": "December",
    "января": "January", "февраля": "February", "марта": "March",
    "апреля": "April", "мая": "May", "июня": "June",
    "июля": "July", "августа": "August", "сентября": "September",
    "октября": "October", "ноября": "November", "декабря": "December",
    "Marchа": "March", "Augustа": "August"
}

#################################
#FUNCTIONS PART
#################################

#################################
#UNIVERSAL FUNCTIONS
#################################

def extract_year(row):
    for col in ['dates', 'dates_rus']:
        value = row.get(col)
        if pd.notna(value) and isinstance(value, (datetime, date)):
            return value.year
    return None

def extract_datetime(row):
    for col in ['dates', 'dates_rus']:
        value = row.get(col)
        if pd.notna(value) and isinstance(value, (datetime, date)):
            return value
    return None

#################################
#ARMENIAN FUNCTIONS
#################################

#################################
#MFA
#################################

def arm_scrape_base_eng_MFA (headers, scrape_date):

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  for page in tqdm(range(1, 60)): #change the number here, after manual appending
    url = f"https://www.mfa.am/en/news?page={page}"
    #print(url)
    res = scraper.get(url=url, headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')
    news_items=soup.find("div", {"class": "news-list"}).find("ul").find_all("li")

    for i in news_items:
      date = i.find("div", {"class": "date italic"})
      if date.text is None:
        date = None
        dates.append(date)
      else:
        date = date.text.strip()
        date = datetime.strptime(date, '%d %B, %Y').date()
        if date > scrape_date:
          dates.append(date)
        else:
          stop_scraping = True
          break

      link=i.find("a")["href"] if i else None
      links.append(link)
      title=i.find("div").find("a").text.strip() if i else None
      titles.append(title)
      desc_items=i.find("p", {"class": "description"}).text.strip() if i else None
      desc.append(desc_items)

    if stop_scraping:
      break  # exit the outer loop too

  id=[i.split("/")[-1] if i else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df=pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links', 'titles', 'dates', 'descriptions'])

  return df


def arm_scrape_full_eng_MFA(row):

  scraper = cloudscraper.create_scraper()

  link_eng = row['links']
  #print(link_eng)
  try:
    res = scraper.get(url=link_eng, headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text=soup.find("div", {"class": "static-content"}).find_all("p")
    text = [i.text.strip() for i in text]
    combined_text = ' '.join(text)

    # Add a pause
    time.sleep(1)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_eng}: {e}")
    combined_text = 'NA'
    return combined_text

#Russian part

def arm_scrape_base_rus_MFA (headers, scrape_date):#identical to eng version, different are url, column titles and date handling

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  for page in tqdm(range(1, 60)): #change the number here, after manual appending
    url = f"https://www.mfa.am/ru/news?page={page}"
    #print(url)
    res = scraper.get(url=url, headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')
    news_items=soup.find("div", {"class": "news-list"}).find("ul").find_all("li")

    for i in news_items:
      date = i.find("div", {"class": "date italic"})
      if date.text is None:
        date = None
        dates.append(date)
      else:
        date = date.text.strip()
        for ru, en in russian_months.items():
          if ru in date:
            date = date.replace(ru, en)
        date = datetime.strptime(date, '%d %B, %Y').date()
        if date > scrape_date:
          dates.append(date)
        else:
          stop_scraping = True
          break

      link=i.find("a")["href"] if i else None
      links.append(link)
      title=i.find("div").find("a").text.strip() if i else None
      titles.append(title)
      desc_items=i.find("p", {"class": "description"}).text.strip() if i else None
      desc.append(desc_items)

    if stop_scraping:
      break  # exit the outer loop too

  id=[i.split("/")[-1] if i else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df=pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links_rus', 'titles_rus', 'dates_rus', 'descriptions_rus'])

  return df


def arm_scrape_full_rus_MFA(row):

  scraper = cloudscraper.create_scraper()

  link_rus = row['links_rus']
  #print(link_eng)
  try:
    res = scraper.get(url=link_rus, headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text=soup.find("div", {"class": "static-content"}).find_all("p")
    text = [i.text.strip() for i in text]
    combined_text = ' '.join(text)

    # Add a pause
    time.sleep(1)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_rus}: {e}")
    combined_text = 'NA'
    return combined_text

#################################
#PRIME MINISTER
#################################


def arm_scrape_base_eng_PM (headers, scrape_date):

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  types_list = ["interviews-and-press-conferences", "press-release", "statements-and-messages"]

  for item_type in types_list:

    for i in tqdm(range(1, 60)):
      #print(item_type)
      url=f"https://www.primeminister.am/en/{item_type}/page/{i}/"
      res = scraper.get(url=url, headers=headers)  # Send GET request to the link
      soup = BeautifulSoup(res.text, 'lxml')
      news_items=soup.find("ul", {"class": "search__list"})

      if news_items is None:
        break

      else:
        news_items = news_items.find_all("li")

        for i in news_items:
          date = i.find("div", {"class": "search__date fs12"})
          if date.text is None:
            date = None
            dates.append(date)
          else:
            date = date.text.strip()
            date = datetime.strptime(date, '%d.%m.%Y').date()
            if date > scrape_date:
              dates.append(date)
            else:
              stop_scraping = True
              break

          link=i.find("a")["href"] if i else None
          links.append(link)
          title=i.find("div", {"class": "search__text-wrapper"}).find("strong").text.strip() if i else None
          titles.append(title)
          desc_items=i.find("div", {"class": "search__text-wrapper"}).find("p").text.strip() if i else None
          desc.append(desc_items)

      if stop_scraping:
        break  # exit the outer loop too

  id = ["/".join(i.split("/")[2:-1]) if i else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df = pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links', 'titles', 'dates', 'descriptions'])

  return df


def arm_scrape_full_eng_PM(row):

  scraper = cloudscraper.create_scraper()

  link_eng = row['links']
  #print(link_eng)
  try:
    res = scraper.get(url=f"https://www.primeminister.am/{link_eng}", headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text=soup.find("div", {"class": "news-text"}).find_all("p")
    text = [i.text.strip() for i in text]
    combined_text = ' '.join(text)
    #print(combined_text[:100])
    # Add a pause
    time.sleep(1)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_eng}: {e}")
    combined_text = 'NA'
    return combined_text

#Russian part

def arm_scrape_base_rus_PM (headers, scrape_date):

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  types_list = ["interviews-and-press-conferences", "press-release", "statements-and-messages"]

  for item_type in types_list:

    for i in tqdm(range(1, 60)):

      url=f"https://www.primeminister.am/ru/{item_type}/page/{i}/"
      res = scraper.get(url=url, headers=headers)  # Send GET request to the link
      soup = BeautifulSoup(res.text, 'lxml')
      news_items=soup.find("ul", {"class": "search__list"})

      if news_items is None:
        break

      else:
        news_items = news_items.find_all("li")

        for i in news_items:
          date = i.find("div", {"class": "search__date fs12"})
          if date.text is None:
            date = None
            dates.append(date)
          else:
            date = date.text.strip()
            date = datetime.strptime(date, '%d.%m.%Y').date()
            if date > scrape_date:
              dates.append(date)
            else:
              stop_scraping = True
              break

          link=i.find("a")["href"] if i else None
          links.append(link)
          title=i.find("div", {"class": "search__text-wrapper"}).find("strong").text.strip() if i else None
          titles.append(title)
          desc_items=i.find("div", {"class": "search__text-wrapper"}).find("p").text.strip() if i else None
          desc.append(desc_items)

      if stop_scraping:
        break  # exit the outer loop too

  id = ["/".join(i.split("/")[2:-1]) if i else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df = pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links_rus', 'titles_rus', 'dates_rus', 'descriptions_rus'])

  return df


def arm_scrape_full_rus_PM(row):

  scraper = cloudscraper.create_scraper()

  link_rus = row['links_rus']
  #print(link_eng)
  try:
    res = scraper.get(url=f"https://www.primeminister.am/{link_rus}", headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text=soup.find("div", {"class": "news-text"}).find_all("p")
    text = [i.text.strip() for i in text]
    combined_text = ' '.join(text)
    #print(combined_text[:100])
    # Add a pause
    time.sleep(1)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_rus}: {e}")
    combined_text = 'NA'
    return combined_text

#################################
#AZERBAIJAN
#################################

def aze_scrape_base_eng_MFA (scrape_date):

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  for page in tqdm(range(1, 60)): #change the number here, after manual appending
    url = f"https://www.mfa.gov.az/en/news?page={page}"
    #print(url)
    res = scraper.get(url=url)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')
    news_items=soup.find_all("div", {"class": "section_content"})[1].find_all("a", {"class": "section_newsIn_item"})

    for i in news_items:
      date = i.find("div", {"class": "section_newsIn_item_content_information"}).find_all("span")[1]
      if date.text is None:
        date = None
        dates.append(date)
      else:
        date = date.text.strip()
        date = datetime.strptime(date, '%d %B %Y').date()
        if date > scrape_date:
          dates.append(date)
          link=i["href"] if i else None
          links.append(link)
          title=i.find("div", {'class': 'section_newsIn_item_content--header'}).text.strip() if i else None
          titles.append(title)
          desc_items=i.find("div", {"class": "section_newsIn_item_content_information"}).find_all("span")[0].text.strip() if i else None
          desc.append(desc_items)
          time.sleep(1)

        else:
          stop_scraping = True
          break

    if stop_scraping:
      break  # exit the outer loop too

  id=[i.split("/")[-1] if i else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df=pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links', 'titles', 'dates', 'descriptions'])

  return df

def aze_scrape_full_eng_MFA(row):

  scraper = cloudscraper.create_scraper()

  link_eng = row['links']
  #print(link_eng)
  try:
    res = scraper.get(url=link_eng)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text=soup.find("div", {"class": "section_articleInner_body_content--description"}).find_all("p")
    text = [i.text.strip() for i in text]
    combined_text = ' '.join(text)
    #print(combined_text[:20])

    # Add a pause
    time.sleep(1)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_eng}: {e}")
    combined_text = 'NA'
    return combined_text


def aze_scrape_base_eng_PRZ (headers, scrape_date):

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  for page in tqdm(range(1, 80)): #change the number here, after manual appending
    url = f"https://president.az/en/news/category/all/{page}"
    #print(url)
    res = scraper.get(url=url)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')
    news_items=soup.find("div", {"class": "news-feed_feed"}).find_all("article")

    for i in news_items:
      cat = i.get('class')[0]
      #print(cat)
      date = i.find("div", {"class": f"{cat}_category"}).find("span", {"class": "category_date"})
      if date.text is None:
        date = None
        dates.append(date)
      else:
        date = list(date.stripped_strings)[0]
        #print(date)
        date = datetime.strptime(date, '%d %B %Y').date()
        if date > scrape_date:
          dates.append(date)
          link=i.find("a")["href"] if i.find("a")["href"] else None
          link = f"https://president.az{link}" if link else None
          links.append(link)
          title_tag = i.find("a")
          title = list(title_tag.stripped_strings)[0] if title_tag and list(title_tag.stripped_strings) else None
          titles.append(title)
          desc_items=None
          desc.append(desc_items)
          time.sleep(1)
        else:
          stop_scraping = True
          break

    if stop_scraping:
        break  # exit the outer loop too

  id = [i.split("/")[-1] if i and i.split("/")[-1] else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df=pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links', 'titles', 'dates', 'descriptions'])

  return df

def aze_scrape_base_rus_PRZ (headers, scrape_date):

  scraper = cloudscraper.create_scraper()

  links, titles, dates, desc = [], [], [], []

  stop_scraping = False

  for page in tqdm(range(1, 80)): #change the number here, after manual appending
    url = f"https://president.az/ru/news/category/all/{page}"
        #print(url)
    res = scraper.get(url=url)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')
    news_items=soup.find("div", {"class": "news-feed_feed"}).find_all("article")

    for i in news_items:
      cat = i.get('class')[0]
      date = i.find("div", {"class": f"{cat}_category"}).find("span", {"class": "category_date"})
      if date.text is None:
        date = None
        dates.append(date)
      else:
        date = list(date.stripped_strings)[0]
        #print(date)
        for ru, en in russian_months.items():
          if ru in date:
            date = date.replace(ru, en)
        date = datetime.strptime(date, '%d %B %Y').date()
        if date > scrape_date:
          dates.append(date)
          link=i.find("a")["href"] if i.find("a")["href"] else None
          link = f"https://president.az{link}" if link else None
          links.append(link)
          title_tag = i.find("a")
          title = list(title_tag.stripped_strings)[0] if title_tag and list(title_tag.stripped_strings) else None
          titles.append(title)
          desc_items=None
          desc.append(desc_items)
          time.sleep(1)

        else:
          stop_scraping = True
          break

    if stop_scraping:
        break  # exit the outer loop too

  id = [i.split("/")[-1] if i and i.split("/")[-1] else None for i in links]

  #Here we use another library - pandas - which allows us to combine several lists into the dataframe
  df=pd.DataFrame(list(zip(id, links, titles, dates, desc)),
              columns=['id', 'links_rus', 'titles_rus', 'dates_rus', 'descriptions_rus'])

  return df


def aze_scrape_full_eng_PRZ(row):

  scraper = cloudscraper.create_scraper() 
  #print(row['links'])
  link_eng = row['links']
  #link_eng = f"https://president.az{row['url']}"
  #print(link_eng)
  try:
    res = scraper.get(url=link_eng, headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text_block=soup.find("div", {"class": "news_paragraph-block"})
    #print(text)
    paragraphs = text_block.find_all("p") if text_block else []
    text = [p.get_text(strip=True) for p in paragraphs]
    combined_text = ' '.join(text)

    # Add a pause
    time.sleep(3)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_eng}: {e}")
    combined_text = 'NA'
    return combined_text


def aze_scrape_full_rus_PRZ(row):

  scraper = cloudscraper.create_scraper()
  #print(row['links'])
  #link_eng = f"https://president.az{row['url_rus']}"
  link_eng = row['links_rus']
  #print(link_eng)
  try:
    res = scraper.get(url=link_eng, headers=headers)  # Send GET request to the link
    soup = BeautifulSoup(res.text, 'lxml')

    # Extract and combine text
    text_block=soup.find("div", {"class": "news_paragraph-block"})
    #print(text)
    paragraphs = text_block.find_all("p") if text_block else []
    text = [p.get_text(strip=True) for p in paragraphs]
    combined_text = ' '.join(text)

    # Add a pause
    time.sleep(3)

    return combined_text

  except Exception as e:
    print(f"Error extracting text from {link_eng}: {e}")
    combined_text = 'NA'
    return combined_text

def main(request):

    last_scrape_date = gdrive.download_txt_file(scrape_id)
    last_scrape_date = last_scrape_date.date()
    print(last_scrape_date)

    tqdm.pandas()

    #################################
    #ARMENIA
    #################################

    df_mfa_eng = arm_scrape_base_eng_MFA(headers=headers, scrape_date=last_scrape_date)
    df_mfa_eng['full_text_eng'] = df_mfa_eng.progress_apply(arm_scrape_full_eng_MFA, axis=1)

    df_mfa_rus = arm_scrape_base_rus_MFA(headers=headers, scrape_date=last_scrape_date)
    df_mfa_rus['full_text_rus'] = df_mfa_rus.progress_apply(arm_scrape_full_rus_MFA, axis=1)

    result_mfa_arm=pd.merge(df_mfa_eng, df_mfa_rus, on='id', how='outer')

    df_pm_eng = arm_scrape_base_eng_PM(headers, last_scrape_date)
    df_pm_eng['full_text_eng'] = df_pm_eng.progress_apply(arm_scrape_full_eng_PM, axis=1)

    df_pm_rus = arm_scrape_base_rus_PM(headers, last_scrape_date)
    df_pm_rus['full_text_rus'] = df_pm_rus.progress_apply(arm_scrape_full_rus_PM, axis=1)

    result_pm_arm=pd.merge(df_pm_eng, df_pm_rus, on='id', how='outer')

    ArmEng_complete = pd.concat([result_mfa_arm, result_pm_arm], axis=0, ignore_index=True)

    ArmEng_complete['years'] = ArmEng_complete.apply(extract_year, axis=1)
    ArmEng_complete['date_datetime'] = ArmEng_complete.apply(extract_datetime, axis=1)

    # Load the DataFrame into a JSON string
    json_data_arm = ArmEng_complete.to_json(orient='records')

    gdrive.save_json_to_drive(json_data_arm, folder_id_json, file_name_arm)

    #################################
    #AZERBAIJAN
    #################################

    result_mfa_aze = aze_scrape_base_eng_MFA(last_scrape_date)
    result_mfa_aze['full_text_eng'] = result_mfa_aze.progress_apply(aze_scrape_full_eng_MFA, axis=1)

    result_pz_aze_eng = aze_scrape_base_eng_PRZ(headers=headers, scrape_date=last_scrape_date)
    result_pz_aze_rus = aze_scrape_base_rus_PRZ(headers=headers, scrape_date=last_scrape_date)

    result_pz_aze=pd.merge(result_pz_aze_eng, result_pz_aze_rus, on='id', how='outer')
    result_pz_aze

    result_pz_aze['full_text_eng'] = result_pz_aze.progress_apply(aze_scrape_full_eng_PRZ, axis=1)
    result_pz_aze['full_text_rus'] = result_pz_aze.progress_apply(aze_scrape_full_rus_PRZ, axis=1)

    result_pz_aze=result_pz_aze.rename(columns={
        'posted_at_date': 'dates',
        'name': 'titles',
        'body': 'descriptions'
    })

    AzeEng_complete = pd.concat([result_mfa_aze, result_pz_aze], axis=0, ignore_index=True)

    AzeEng_complete['years'] = AzeEng_complete.apply(extract_year, axis=1)
    AzeEng_complete['date_datetime'] = AzeEng_complete.apply(extract_datetime, axis=1)

    # Load the DataFrame into a JSON string
    json_data_aze = AzeEng_complete.to_json(orient='records')

    gdrive.save_json_to_drive(json_data_aze, folder_id_json, file_name_aze)

    #################################
    #UPDATE THE SCRAPE TIME
    #################################

    gmt_now = datetime.now(pytz.utc)
    target_tz = pytz.timezone('Asia/Yerevan')
    local_time = gmt_now.astimezone(target_tz)
    new_scrape_time = local_time.strftime("%d/%m/%Y %H:%M:%S")
    print(new_scrape_time)
    gdrive.upload_txt_file(scrape_id, new_scrape_time)

    return "Scraping complete", 200
