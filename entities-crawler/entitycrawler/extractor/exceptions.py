class ExtractionError(Exception):

    def __init__(self, url, scrapped_page=None):
        self.url = url
        self.scrapped_page = scrapped_page
        self.message = "Couldn't make extraction for: [%s]\nScrapped page:\n%s" % (url, scrapped_page)
        super(ExtractionError, self).__init__(self.message)
