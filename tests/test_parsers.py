from scrapling import Selector

from fmkorea_scraper.collect.parsers import parse_board_page
from fmkorea_scraper.models.job import CrawlConfig, RealtimeTermination
from fmkorea_scraper.parse.service import parse_post


def test_board_and_post_contract():
    listing = Selector('''<div class="bd"><table><tbody>
      <tr class="notice"><td class="title"><a href="/1">notice</a></td></tr>
      <tr><td class="title"><a href="/12345">title</a></td><td class="time">2026.10.12 15:35</td></tr>
    </tbody></table><a href="?page=2">2</a></div>''')
    page = parse_board_page(listing, "https://www.fmkorea.com/humor", 1, "humor")
    assert page.has_next and [post.document_srl for post in page.posts] == [12345]

    html = '''<html><head><link rel="canonical" href="https://www.fmkorea.com/12345"></head>
      <body><a href="/index.php?mid=humor&document_srl=12345">board</a>
      <div id="bd_capture"><div class="rd_hd"><h1><span class="np_18px_span">title</span></h1>
      <div class="btm_area"><a class="member_12 member_plate">writer<img class="level" alt="[레벨:20]"></a>
      <div class="side fr"><span>조회 수 <b>66</b></span><span>추천 수 <b>2</b></span><span>댓글 <b>1</b></span></div></div>
      <span class="date">2026.10.12 15:35</span></div><div class="rd_body"><article>
      <div class="xe_content"><p>hello <a class="highslide" href="//image.fmkorea.com/x.jpg"><img src="//image.fmkorea.com/x.jpg" data-file-srl="9"></a> world</p></div>
      </article></div></div><ul class="fdb_lst_ul"><li id="comment_7_"><div class="meta"><a class="member_plate">reader</a></div><div class="xe_content">reply</div></li></ul>
      </body></html>'''
    post, warnings = parse_post(html, "https://www.fmkorea.com/12345")
    assert post.board_id == "humor" and post.reactions.score == 2
    assert post.content.blocks[0].type == "text"
    assert post.media[0].url == "https://image.fmkorea.com/x.jpg"
    assert post.comments[0].comment_srl == 7
    assert not warnings


def test_realtime_and_retry_validation():
    try:
        RealtimeTermination()
    except ValueError:
        pass
    else:
        assert False, "termination must reject missing deadline"
    try:
        CrawlConfig(max_blocked_retries=2)
    except ValueError:
        pass
    else:
        assert False, "blocked retry budget must finish on Browser"
