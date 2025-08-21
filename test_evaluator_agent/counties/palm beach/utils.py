from bs4 import BeautifulSoup

def find_area_table(soup):
    """
    Find the 'SUBAREA AND SQUARE FOOTAGE' table in the HTML.
    
    Args:
        soup: BeautifulSoup object of the HTML content
        
    Returns:
        The area table element if found, None otherwise
    """
    all_h3 = soup.find_all('h3')
    area_table = None
    for h3 in all_h3:
        if 'SUBAREA AND SQUARE FOOTAGE' in h3.get_text(strip=True):
            area_table = h3
            break
    
    if area_table:
        area_table = area_table.find_next('table')
        return area_table
    
    return None


def find_structural_elements_tables(soup):
    """
    Find all 'structural_elements' tables in the HTML.
    
    Args:
        soup: BeautifulSoup object of the HTML content
        
    Returns:
        List of structural_elements table elements
    """
    return soup.find_all("table", class_="structural_elements")
