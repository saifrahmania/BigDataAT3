import glob

header = "listing_id,scrape_id,scraped_date,host_id,host_name,host_since,host_is_superhost,host_neighbourhood,listing_neighbourhood,property_type,room_type,accommodates,price,has_availability,availability_30,number_of_reviews,review_scores_rating,review_scores_accuracy,review_scores_cleanliness,review_scores_checkin,review_scores_communication,review_scores_value,source_file,year_month\n"

for file in sorted(glob.glob("listings_raw_part_*.csv")):
    with open(file, "r", encoding="utf-8") as f:
        content = f.read()
    if not content.startswith("listing_id,"):
        with open(file, "w", encoding="utf-8") as f:
            f.write(header + content)
        print(f"✅ Fixed header in {file}")
    else:
        print(f"✅ Already has header: {file}")