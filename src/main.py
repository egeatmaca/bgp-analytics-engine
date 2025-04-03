from update_loader import UpdateLoader 

if __name__ == '__main__':
    loader = UpdateLoader(
        collectors=['route-views.eqix'],
        from_time="2022-07-07 00:00:00", 
        until_time="2022-07-14 00:00:00"
    )
    loader.load_updates()
