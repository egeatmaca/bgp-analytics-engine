from bgpstream_loader import BGPStreamLoader 


if __name__ == '__main__':
    loader = BGPStreamLoader(
        collectors=['rrc00'], # TODO: Set collector
        from_time="2024-07-07 00:00:00", 
        until_time="2024-07-08 00:00:00"
    )
    loader.load_updates()
