import scrapy
import pandas as pd
import numpy as np

class CountriesSpider(scrapy.Spider):
    name = "countries"
    # Allowed domains doesn't support the tag http or https
    allowed_domains = ["www.worldometers.info"]
    start_urls = ["https://www.worldometers.info/world-population/population-by-country/"]

    def __init__(self):
        super().__init__()
        self.data = []

    def parse(self, response):

        # Get all the countries in the table
        countries = response.xpath('//td/a')

        for country in countries:
            # Start with .// because starting an operation from a selector object and not a response object
            name = country.xpath(".//text()").get()
            link = country.xpath('.//@href').get()

            # Send the response as a parameter for the parse_country callback, add the country name as a meta data
            yield response.follow(link, callback=self.parse_country, meta={'country_name': name})

    def parse_country(self, response):

        # Find the right table with the historical population data
        rows = response.xpath("(//table[@class='table table-striped table-bordered table-hover table-condensed table-list'])[1]/tbody/tr")

        for row in rows:
            # Save and yield every year and population
            year = row.xpath('.//td[1]/text()').get()
            population = row.xpath(".//td[2]/strong/text()").get()

            # Store the year and population under the country's entry
            self.data.append({
                "country_name": response.meta['country_name'],
                "year": year,
                "population": population
            })

    def closed(self, reason):

        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(self.data)

        # Pivot the DataFrame to have countries as columns, year as index, and population as values
        df = df.pivot(index='year', columns='country_name', values='population')

        # Remove all , from populations
        df = df.applymap(lambda x: x.replace(',', '') if isinstance(x, str) else x)

        # Save this DataFrame to a CSV file
        df.to_csv("population_data.csv")

        # See the result in the console
        print(df)