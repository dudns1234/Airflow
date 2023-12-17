import requests
from bs4 import BeautifulSoup

def get_musinsa_product():
    
    URL = "https://www.musinsa.com/category/001001"

    response = requests.get(URL)
    page = response.content
    soup = BeautifulSoup(page, 'html.parser')

    # 제품 list 접근
    product_list = soup.select("#searchList > li")

    data_list = []

    # 브랜드, 상품명, 가격, 별점등록수, 없으면 처리해주기
    for product in product_list:
        img_url = product.select_one("img.lazyload")['data-original']

        # 브랜드
        if len(product.select("p.item_title")) > 1:
            product_title = product.select("p.item_title")[-1].text
        else:
            product_title = product.select_one("p.item_title").text
            
        # 상품명
        product_name = product.select_one("p.list_info").text.strip()

        # 가격
        price_del = product.select_one("p.price").find("del")

        if price_del:
            product_price = price_del.nextSibling.strip()
        else:
            product_price = product.select_one("p.price").text.strip()
            
        product_price = int(product_price.replace(",","").replace("원",""))

        # 별점 등록 수
        point_count = product.select_one("p.point > span.count")

        if point_count is not None:
            star_count = int(point_count.text.replace(",", ""))
        else:
            star_count = int(0)
        
        data = {
            "product_title" : product_title,  # 브랜드
            "product_name" : product_name,    # 상품명
            "product_price" : product_price,  # 가격
            "star_count" : star_count,        # 별점등록수
            "img_url" : img_url               # 이미지 url
        }

        data_list.append(data)

        #print(data_list)

    # XCOM을 활용하기 위한 리턴!!!!!!!!
    return data_list
    
        
