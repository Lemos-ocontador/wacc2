# -*- coding: utf-8 -*-
"""
Mapeamentos geográficos e setoriais para filtros hierárquicos
Baseado na classificação geográfica da ONU (M49) e padrões industriais
"""

# Mapeamento de países para regiões e sub-regiões baseado na ONU
GEOGRAPHIC_MAPPING = {
    # AMERICAS
    "United States": {"region": "Americas", "subregion": "Northern America"},
    "Canada": {"region": "Americas", "subregion": "Northern America"},
    "Mexico": {"region": "Americas", "subregion": "Central America"},
    "Brazil": {"region": "Americas", "subregion": "South America"},
    "Argentina": {"region": "Americas", "subregion": "South America"},
    "Chile": {"region": "Americas", "subregion": "South America"},
    "Colombia": {"region": "Americas", "subregion": "South America"},
    "Peru": {"region": "Americas", "subregion": "South America"},
    "Venezuela": {"region": "Americas", "subregion": "South America"},
    "Ecuador": {"region": "Americas", "subregion": "South America"},
    "Uruguay": {"region": "Americas", "subregion": "South America"},
    "Paraguay": {"region": "Americas", "subregion": "South America"},
    "Bolivia": {"region": "Americas", "subregion": "South America"},
    "Guyana": {"region": "Americas", "subregion": "South America"},
    "Suriname": {"region": "Americas", "subregion": "South America"},
    
    # ASIA
    "China": {"region": "Asia", "subregion": "Eastern Asia"},
    "Japan": {"region": "Asia", "subregion": "Eastern Asia"},
    "South Korea": {"region": "Asia", "subregion": "Eastern Asia"},
    "Taiwan": {"region": "Asia", "subregion": "Eastern Asia"},
    "Hong Kong": {"region": "Asia", "subregion": "Eastern Asia"},
    "Mongolia": {"region": "Asia", "subregion": "Eastern Asia"},
    "North Korea": {"region": "Asia", "subregion": "Eastern Asia"},
    
    "India": {"region": "Asia", "subregion": "Southern Asia"},
    "Pakistan": {"region": "Asia", "subregion": "Southern Asia"},
    "Bangladesh": {"region": "Asia", "subregion": "Southern Asia"},
    "Sri Lanka": {"region": "Asia", "subregion": "Southern Asia"},
    "Nepal": {"region": "Asia", "subregion": "Southern Asia"},
    "Bhutan": {"region": "Asia", "subregion": "Southern Asia"},
    "Maldives": {"region": "Asia", "subregion": "Southern Asia"},
    "Afghanistan": {"region": "Asia", "subregion": "Southern Asia"},
    
    "Thailand": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Vietnam": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Indonesia": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Malaysia": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Singapore": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Philippines": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Myanmar": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Cambodia": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Laos": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Brunei": {"region": "Asia", "subregion": "South-Eastern Asia"},
    "Timor-Leste": {"region": "Asia", "subregion": "South-Eastern Asia"},
    
    "Turkey": {"region": "Asia", "subregion": "Western Asia"},
    "Israel": {"region": "Asia", "subregion": "Western Asia"},
    "Saudi Arabia": {"region": "Asia", "subregion": "Western Asia"},
    "United Arab Emirates": {"region": "Asia", "subregion": "Western Asia"},
    "Iran": {"region": "Asia", "subregion": "Western Asia"},
    "Iraq": {"region": "Asia", "subregion": "Western Asia"},
    "Jordan": {"region": "Asia", "subregion": "Western Asia"},
    "Lebanon": {"region": "Asia", "subregion": "Western Asia"},
    "Syria": {"region": "Asia", "subregion": "Western Asia"},
    "Kuwait": {"region": "Asia", "subregion": "Western Asia"},
    "Qatar": {"region": "Asia", "subregion": "Western Asia"},
    "Bahrain": {"region": "Asia", "subregion": "Western Asia"},
    "Oman": {"region": "Asia", "subregion": "Western Asia"},
    "Yemen": {"region": "Asia", "subregion": "Western Asia"},
    "Cyprus": {"region": "Asia", "subregion": "Western Asia"},
    "Georgia": {"region": "Asia", "subregion": "Western Asia"},
    "Armenia": {"region": "Asia", "subregion": "Western Asia"},
    "Azerbaijan": {"region": "Asia", "subregion": "Western Asia"},
    
    # EUROPE
    "United Kingdom": {"region": "Europe", "subregion": "Northern Europe"},
    "Ireland": {"region": "Europe", "subregion": "Northern Europe"},
    "Denmark": {"region": "Europe", "subregion": "Northern Europe"},
    "Sweden": {"region": "Europe", "subregion": "Northern Europe"},
    "Norway": {"region": "Europe", "subregion": "Northern Europe"},
    "Finland": {"region": "Europe", "subregion": "Northern Europe"},
    "Iceland": {"region": "Europe", "subregion": "Northern Europe"},
    "Estonia": {"region": "Europe", "subregion": "Northern Europe"},
    "Latvia": {"region": "Europe", "subregion": "Northern Europe"},
    "Lithuania": {"region": "Europe", "subregion": "Northern Europe"},
    
    "Germany": {"region": "Europe", "subregion": "Western Europe"},
    "France": {"region": "Europe", "subregion": "Western Europe"},
    "Netherlands": {"region": "Europe", "subregion": "Western Europe"},
    "Belgium": {"region": "Europe", "subregion": "Western Europe"},
    "Luxembourg": {"region": "Europe", "subregion": "Western Europe"},
    "Switzerland": {"region": "Europe", "subregion": "Western Europe"},
    "Austria": {"region": "Europe", "subregion": "Western Europe"},
    "Liechtenstein": {"region": "Europe", "subregion": "Western Europe"},
    "Monaco": {"region": "Europe", "subregion": "Western Europe"},
    
    "Italy": {"region": "Europe", "subregion": "Southern Europe"},
    "Spain": {"region": "Europe", "subregion": "Southern Europe"},
    "Portugal": {"region": "Europe", "subregion": "Southern Europe"},
    "Greece": {"region": "Europe", "subregion": "Southern Europe"},
    "Malta": {"region": "Europe", "subregion": "Southern Europe"},
    "San Marino": {"region": "Europe", "subregion": "Southern Europe"},
    "Vatican City": {"region": "Europe", "subregion": "Southern Europe"},
    "Andorra": {"region": "Europe", "subregion": "Southern Europe"},
    "Albania": {"region": "Europe", "subregion": "Southern Europe"},
    "Bosnia and Herzegovina": {"region": "Europe", "subregion": "Southern Europe"},
    "Croatia": {"region": "Europe", "subregion": "Southern Europe"},
    "Montenegro": {"region": "Europe", "subregion": "Southern Europe"},
    "North Macedonia": {"region": "Europe", "subregion": "Southern Europe"},
    "Serbia": {"region": "Europe", "subregion": "Southern Europe"},
    "Slovenia": {"region": "Europe", "subregion": "Southern Europe"},
    
    "Poland": {"region": "Europe", "subregion": "Eastern Europe"},
    "Czech Republic": {"region": "Europe", "subregion": "Eastern Europe"},
    "Slovakia": {"region": "Europe", "subregion": "Eastern Europe"},
    "Hungary": {"region": "Europe", "subregion": "Eastern Europe"},
    "Romania": {"region": "Europe", "subregion": "Eastern Europe"},
    "Bulgaria": {"region": "Europe", "subregion": "Eastern Europe"},
    "Russia": {"region": "Europe", "subregion": "Eastern Europe"},
    "Ukraine": {"region": "Europe", "subregion": "Eastern Europe"},
    "Belarus": {"region": "Europe", "subregion": "Eastern Europe"},
    "Moldova": {"region": "Europe", "subregion": "Eastern Europe"},
    
    # OCEANIA
    "Australia": {"region": "Oceania", "subregion": "Australia and New Zealand"},
    "New Zealand": {"region": "Oceania", "subregion": "Australia and New Zealand"},
    "Fiji": {"region": "Oceania", "subregion": "Melanesia"},
    "Papua New Guinea": {"region": "Oceania", "subregion": "Melanesia"},
    "Solomon Islands": {"region": "Oceania", "subregion": "Melanesia"},
    "Vanuatu": {"region": "Oceania", "subregion": "Melanesia"},
    "New Caledonia": {"region": "Oceania", "subregion": "Melanesia"},
    
    # AFRICA
    "South Africa": {"region": "Africa", "subregion": "Southern Africa"},
    "Nigeria": {"region": "Africa", "subregion": "Western Africa"},
    "Egypt": {"region": "Africa", "subregion": "Northern Africa"},
    "Kenya": {"region": "Africa", "subregion": "Eastern Africa"},
    "Morocco": {"region": "Africa", "subregion": "Northern Africa"},
    "Ghana": {"region": "Africa", "subregion": "Western Africa"},
    "Tunisia": {"region": "Africa", "subregion": "Northern Africa"},
    "Algeria": {"region": "Africa", "subregion": "Northern Africa"},
    "Libya": {"region": "Africa", "subregion": "Northern Africa"},
    "Sudan": {"region": "Africa", "subregion": "Northern Africa"},
    "Ethiopia": {"region": "Africa", "subregion": "Eastern Africa"},
    "Tanzania": {"region": "Africa", "subregion": "Eastern Africa"},
    "Uganda": {"region": "Africa", "subregion": "Eastern Africa"},
    "Rwanda": {"region": "Africa", "subregion": "Eastern Africa"},
    "Botswana": {"region": "Africa", "subregion": "Southern Africa"},
    "Namibia": {"region": "Africa", "subregion": "Southern Africa"},
    "Zambia": {"region": "Africa", "subregion": "Southern Africa"},
    "Zimbabwe": {"region": "Africa", "subregion": "Southern Africa"},
    "Malawi": {"region": "Africa", "subregion": "Southern Africa"},
    "Mozambique": {"region": "Africa", "subregion": "Southern Africa"},
}

# Mapeamento de indústrias para setores hierárquicos
INDUSTRY_MAPPING = {
    # TECHNOLOGY
    "Software (System & Application)": {"sector": "Technology", "subsector": "Software"},
    "Computer Services": {"sector": "Technology", "subsector": "IT Services"},
    "Electronics (General)": {"sector": "Technology", "subsector": "Electronics"},
    "Electrical Equipment": {"sector": "Technology", "subsector": "Electronics"},
    "Semiconductor": {"sector": "Technology", "subsector": "Semiconductors"},
    
    # HEALTHCARE
    "Drugs (Pharmaceutical)": {"sector": "Healthcare", "subsector": "Pharmaceuticals"},
    "Drugs (Biotechnology)": {"sector": "Healthcare", "subsector": "Biotechnology"},
    "Healthcare Products": {"sector": "Healthcare", "subsector": "Medical Devices"},
    
    # FINANCIAL SERVICES
    "Banks (Regional)": {"sector": "Financial Services", "subsector": "Banking"},
    "Financial Svcs. (Non-bank & Insurance)": {"sector": "Financial Services", "subsector": "Insurance"},
    "Investments & Asset Management": {"sector": "Financial Services", "subsector": "Asset Management"},
    "R.E.I.T.": {"sector": "Financial Services", "subsector": "Real Estate Investment"},
    
    # INDUSTRIALS
    "Engineering/Construction": {"sector": "Industrials", "subsector": "Construction"},
    "Machinery": {"sector": "Industrials", "subsector": "Machinery"},
    "Transportation": {"sector": "Industrials", "subsector": "Transportation"},
    "Auto Parts": {"sector": "Industrials", "subsector": "Automotive"},
    
    # MATERIALS
    "Metals & Mining": {"sector": "Materials", "subsector": "Mining"},
    "Steel": {"sector": "Materials", "subsector": "Steel"},
    "Precious Metals": {"sector": "Materials", "subsector": "Precious Metals"},
    "Chemical (Basic)": {"sector": "Materials", "subsector": "Basic Chemicals"},
    "Chemical (Specialty)": {"sector": "Materials", "subsector": "Specialty Chemicals"},
    
    # CONSUMER
    "Food Processing": {"sector": "Consumer Goods", "subsector": "Food & Beverages"},
    "Apparel": {"sector": "Consumer Goods", "subsector": "Apparel"},
    "Retail (Distributors)": {"sector": "Consumer Goods", "subsector": "Retail"},
    "Retail (Special Lines)": {"sector": "Consumer Goods", "subsector": "Specialty Retail"},
    
    # REAL ESTATE
    "Real Estate (Development)": {"sector": "Real Estate", "subsector": "Development"},
    "Real Estate (Operations & Services)": {"sector": "Real Estate", "subsector": "Operations"},
    "Hotel/Gaming": {"sector": "Real Estate", "subsector": "Hospitality"},
    
    # SERVICES
    "Business & Consumer Services": {"sector": "Services", "subsector": "Business Services"},
    "Entertainment": {"sector": "Services", "subsector": "Entertainment"},
}

def get_geographic_hierarchy():
    """Retorna a hierarquia geográfica organizada"""
    hierarchy = {}
    
    for country, data in GEOGRAPHIC_MAPPING.items():
        region = data["region"]
        subregion = data["subregion"]
        
        if region not in hierarchy:
            hierarchy[region] = {}
        
        if subregion not in hierarchy[region]:
            hierarchy[region][subregion] = []
        
        hierarchy[region][subregion].append(country)
    
    return hierarchy

def get_industry_hierarchy():
    """Retorna a hierarquia industrial organizada"""
    hierarchy = {}
    
    for industry, data in INDUSTRY_MAPPING.items():
        sector = data["sector"]
        subsector = data["subsector"]
        
        if sector not in hierarchy:
            hierarchy[sector] = {}
        
        if subsector not in hierarchy[sector]:
            hierarchy[sector][subsector] = []
        
        hierarchy[sector][subsector].append(industry)
    
    return hierarchy

def get_country_region(country):
    """Retorna região e sub-região de um país"""
    return GEOGRAPHIC_MAPPING.get(country, {"region": "Other", "subregion": "Other"})

def get_industry_sector(industry):
    """Retorna setor e subsetor de uma indústria"""
    return INDUSTRY_MAPPING.get(industry, {"sector": "Other", "subsector": "Other"})