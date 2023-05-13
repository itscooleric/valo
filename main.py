from schedule import scrape_vlr_matches

while True:
    print("Please choose an option:")
    print("1. Scrape VLR matches")
    print("2. Exit")

    selection = input("Enter your choice (1 or 2): ")

    if selection == "1":
        num_pages = int(input("Enter the number of pages to scrape: "))
        scrape_vlr_matches(num_pages)
    elif selection == "2":
        break
    else:
        print("Invalid selection. Please try again.\n")