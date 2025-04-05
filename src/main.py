from bgpstream_loader import BGPStreamLoader 


if __name__ == '__main__':
    loader = BGPStreamLoader(
        collectors=['route-views.eqix'],
        from_time="2020-07-07 00:00:00", 
        until_time="2020-07-14 00:00:00"
    )
    loader.load_updates()
