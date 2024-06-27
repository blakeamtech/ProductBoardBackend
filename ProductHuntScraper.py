import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


class ProductHuntScraper:

    def __init__(self):
        pass

    def get_weekly_top_products(self):
        
        date = datetime.now()

        year = date.year
        week_num = date.isocalendar().week

        response = requests.get(f"https://www.producthunt.com/leaderboard/weekly/{year}/{week_num}")

        soup = BeautifulSoup(response.text, "html.parser")

        elements = soup.find_all(class_="styles_item__Dk_nz")

        names = []
        likes = []
        urls = []

        for element in elements:
            strong_tag = element.find('strong')
            product_name = strong_tag.text if strong_tag else "No Product Name"
            names.append(product_name)

            like_class = ['color-light-grey', 'fontSize-12', 'fontWeight-600', 'styles_voteCountItem__zwuqk']
            vote_count_tag = element.find(class_=like_class)
            like_count = vote_count_tag.text if vote_count_tag else "No Vote Count"
            likes.append(int(like_count))

            a_tag = element.find('a')

            if a_tag:
                urls.append(f"https://www.producthunt.com{a_tag.get('href')}")

        descriptions = []

        for url in urls:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            div_text = soup.find("div", class_="styles_htmlText__eYPgj").get_text(strip=True)

            descriptions.append(div_text)

        dataset = list(zip(names, urls, likes, descriptions))
        df = pd.DataFrame(dataset, columns = ['Name', 'URL', 'Likes', 'Description'])
        df['date'] = f"{week_num}-{year}"
        return df

    def get_monthly_top_products(self):
        date = datetime.now()

        year = date.year
        month = date.month

        response = requests.get(f"https://www.producthunt.com/leaderboard/monthly/{year}/{month}")

        soup = BeautifulSoup(response.text, "html.parser")

        elements = soup.find_all(class_="styles_item__Dk_nz")

        names = []
        likes = []
        urls = []

        for element in elements:
            strong_tag = element.find('strong')
            product_name = strong_tag.text if strong_tag else "No Product Name"
            names.append(product_name)

            like_class = ['color-light-grey', 'fontSize-12', 'fontWeight-600', 'styles_voteCountItem__zwuqk']
            vote_count_tag = element.find(class_=like_class)
            like_count = vote_count_tag.text if vote_count_tag else "No Vote Count"
            likes.append(int(like_count))

            a_tag = element.find('a')

            if a_tag:
                urls.append(f"https://www.producthunt.com{a_tag.get('href')}")

        descriptions = []

        for url in urls:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            div_text = soup.find("div", class_="styles_htmlText__eYPgj").get_text(strip=True)

            descriptions.append(div_text)

        dataset = list(zip(names, urls, likes, descriptions))
        df = pd.DataFrame(dataset, columns = ['Name', 'URL', 'Likes', 'Description'])
        
        df['date'] = f"{month}-{year}"
        return df