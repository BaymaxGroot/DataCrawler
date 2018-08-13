from crawlers.server.MovieDouban import Douban


if __name__ == '__main__':
    douban = Douban(10)
    # douban.crawler()
    douban.rtl_movie_detail_info()