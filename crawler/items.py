# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    department = scrapy.Field()
    type = scrapy.Field()
    programAndPlatform = scrapy.Field()
    location = scrapy.Field()
    featured = scrapy.Field()
    level = scrapy.Field()
    creationDate = scrapy.Field()
    team = scrapy.Field()
    portalID = scrapy.Field()
    statusID = scrapy.Field()
    statusName = scrapy.Field()
    updatedDate = scrapy.Field()
    uniqueSkills = scrapy.Field()
    allLocations = scrapy.Field()
