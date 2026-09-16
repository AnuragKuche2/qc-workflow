from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratorConfig:
    catalog: str = "qc_dev"
    schema: str = "bronze_source"
    days: int = 90
    orders_per_day: int = 15_000     # BASELINE for an average day. Real totals run ~6%
                                      # higher because event days are genuinely busier.
    seed: int = 20260908             # Change for a different but equally valid world.
    start_date: str = "2026-06-01"   # a Monday, so day 0 starts a clean week
    n_cities: int = 3
    zones_per_city: int = 15
    # 3,000 riders, not 1,000. At 1,000 the baseline is 15.9 deliveries per rider per day
    # (~13 hours) and the festival is 47 (physically impossible).
    n_restaurants: int = 1_000
    n_riders: int = 3_000
    n_customers: int = 300_000
    menu_items_per_restaurant: int = 25
    cancel_rate: float = 0.06
    unfulfilled_rate: float = 0.025
    refund_rate: float = 0.028           # fraction of DELIVERED orders that also get a
                                          # quality-issue refund
    payment_failure_rate: float = 0.01
    money_text_defect_rate: float = 0.15   # fraction of refunds using accounting-negative
                                             # text formatting
    text_noise_defect_rate: float = 0.08   # fraction of non-null delivery_notes with
                                             # casing/whitespace mangling


HISTORY_DAYS = 730   # entities were created in the 2 years BEFORE day 0

# name, country, timezone, lat, lon, share of the business
CITIES = [
    ("Bengaluru", "IN", "Asia/Kolkata", 12.9716, 77.5946, 0.42),
    ("Mumbai", "IN", "Asia/Kolkata", 19.0760, 72.8777, 0.35),
    ("Delhi", "IN", "Asia/Kolkata", 28.6139, 77.2090, 0.23),
]

# Ordered inner-to-outer: index 0 is the densest zone. Density decays outward, which is
# what creates the hot-zone skew the file-layout work (Sub-project H) will address.
ZONE_NAMES = {
    "Bengaluru": ["Indiranagar", "Koramangala", "HSR Layout", "Jayanagar", "BTM Layout",
                  "Rajajinagar", "Malleshwaram", "Bellandur", "JP Nagar", "Banashankari",
                  "Marathahalli", "Hebbal", "Whitefield", "Yelahanka", "Electronic City"],
    "Mumbai": ["Lower Parel", "Bandra West", "Dadar", "Worli", "Andheri East",
               "Juhu", "Kurla", "Chembur", "Powai", "Goregaon",
               "Malad West", "Colaba", "Borivali", "Vashi", "Thane West"],
    "Delhi": ["Connaught Place", "Karol Bagh", "Hauz Khas", "Lajpat Nagar", "Greater Kailash",
              "Saket", "Nehru Place", "Model Town", "Rajouri Garden", "Pitampura",
              "Janakpuri", "Vasant Kunj", "Mayur Vihar", "Rohini", "Dwarka"],
}

# name, share of restaurants, base commission, p50 prep minutes
CUISINES = [
    ("North Indian", 0.22, 0.1800, 22), ("South Indian", 0.16, 0.1600, 14),
    ("Chinese", 0.14, 0.2000, 18), ("Biryani", 0.13, 0.2200, 26),
    ("Pizza", 0.11, 0.2400, 20), ("Burgers", 0.10, 0.2300, 12),
    ("Desserts", 0.08, 0.1900, 10), ("Healthy Bowls", 0.06, 0.1700, 16),
]

# One 6-item list per cuisine each, used to construct restaurant chain names like
# "Punjabi Dhaba". Fewer brands than outlets on purpose (see build_restaurants) -
# multi-outlet chains make "which outlet of this brand is slowest" a real question.
BRAND_PREFIX = {
    "North Indian": ["Punjabi", "Royal", "Tandoor", "Kesar", "Rajdhani", "Sagar"],
    "South Indian": ["Udupi", "Anand", "Dosa", "Madras", "Coastal", "Nandini"],
    "Chinese": ["Golden", "Wok", "Sichuan", "Chin", "Dragon", "Mandarin"],
    "Biryani": ["Paradise", "Hyderabad", "Bawarchi", "Zaiqa", "Lucknowi", "Nawab"],
    "Pizza": ["Napoli", "Crust", "Forno", "Slice", "Bella", "Roma"],
    "Burgers": ["Grill", "Patty", "Smash", "Bun", "Char", "Stack"],
    "Desserts": ["Sweet", "Cocoa", "Sugar", "Kulfi", "Frost", "Velvet"],
    "Healthy Bowls": ["Green", "Fresh", "Root", "Sprout", "Nourish", "Clean"],
}
BRAND_CORE = {
    "North Indian": ["Dhaba", "Rasoi", "Darbar", "Junction", "Tadka", "Angan"],
    "South Indian": ["Tiffin", "Bhavan", "Corner", "Sagar", "Cafe", "Mess"],
    "Chinese": ["Bowl", "Bay", "Express", "Garden", "Kitchen", "Palace"],
    "Biryani": ["House", "Handi", "Dum", "Mahal", "Point", "Kitchen"],
    "Pizza": ["Pizzeria", "Oven", "Co", "Kitchen", "Bakehouse", "Corner"],
    "Burgers": ["Shack", "Yard", "Bros", "Works", "Company", "Joint"],
    "Desserts": ["Bakery", "Patisserie", "Scoop", "Bar", "Room", "Studio"],
    "Healthy Bowls": ["Bowl", "Kitchen", "Table", "Co", "Greens", "Bar"],
}

# 12 (name, category, price) templates per cuisine. Each restaurant draws 25 items, so
# items repeat across a chain with independently drifting prices - which is what gives a
# later SCD2 price history something non-trivial to track.
MENUS = {
    "North Indian": [("Paneer Butter Masala", "Mains", 280), ("Dal Makhani", "Mains", 240),
                     ("Butter Chicken", "Mains", 360), ("Kadai Paneer", "Mains", 290),
                     ("Chole Bhature", "Mains", 180), ("Butter Naan", "Breads", 60),
                     ("Tandoori Roti", "Breads", 35), ("Laccha Paratha", "Breads", 70),
                     ("Jeera Rice", "Rice", 150), ("Paneer Tikka", "Starters", 260),
                     ("Boondi Raita", "Sides", 90), ("Gulab Jamun", "Desserts", 110)],
    "South Indian": [("Masala Dosa", "Mains", 130), ("Plain Dosa", "Mains", 90),
                     ("Ghee Roast Dosa", "Mains", 170), ("Idli Sambar", "Mains", 80),
                     ("Rava Idli", "Mains", 100), ("Medu Vada", "Starters", 70),
                     ("Upma", "Mains", 85), ("Pongal", "Mains", 110),
                     ("Curd Rice", "Rice", 95), ("Lemon Rice", "Rice", 100),
                     ("Filter Coffee", "Beverages", 45), ("Mysore Pak", "Desserts", 90)],
    "Chinese": [("Veg Hakka Noodles", "Mains", 190), ("Chicken Fried Rice", "Rice", 230),
                ("Chilli Paneer Dry", "Starters", 250), ("Chicken Manchurian", "Mains", 270),
                ("Veg Spring Rolls", "Starters", 160), ("Schezwan Noodles", "Mains", 210),
                ("Chicken Momos", "Starters", 180), ("Sweet Corn Soup", "Starters", 130),
                ("Hot and Sour Soup", "Starters", 140), ("Kung Pao Chicken", "Mains", 300),
                ("Chilli Garlic Rice", "Rice", 200), ("Honey Chilli Potato", "Sides", 170)],
    "Biryani": [("Chicken Dum Biryani", "Mains", 330), ("Mutton Biryani", "Mains", 470),
                ("Veg Biryani", "Mains", 240), ("Egg Biryani", "Mains", 260),
                ("Chicken 65", "Starters", 280), ("Kebab Platter", "Starters", 390),
                ("Mirchi Ka Salan", "Sides", 90), ("Raita", "Sides", 60),
                ("Double Ka Meetha", "Desserts", 130), ("Haleem", "Mains", 340),
                ("Chicken Fry Piece", "Starters", 220), ("Irani Chai", "Beverages", 40)],
    "Pizza": [("Margherita", "Mains", 280), ("Farmhouse", "Mains", 420),
              ("Peppy Paneer", "Mains", 400), ("Chicken Tikka Pizza", "Mains", 480),
              ("Pepperoni", "Mains", 520), ("Garlic Bread", "Sides", 150),
              ("Cheesy Dip", "Sides", 45), ("Chicken Wings", "Starters", 320),
              ("Penne Alfredo", "Mains", 350), ("Choco Lava Cake", "Desserts", 120),
              ("Cold Coffee", "Beverages", 140), ("Caesar Salad", "Starters", 260)],
    "Burgers": [("Classic Veg Burger", "Mains", 150), ("Crispy Chicken Burger", "Mains", 230),
                ("Double Cheese Burger", "Mains", 320), ("Paneer Tikka Burger", "Mains", 220),
                ("Peri Peri Fries", "Sides", 130), ("Salted Fries", "Sides", 100),
                ("Cheese Loaded Fries", "Sides", 190), ("Chicken Popcorn", "Starters", 180),
                ("Onion Rings", "Sides", 140), ("Chocolate Shake", "Beverages", 180),
                ("Iced Tea", "Beverages", 110), ("Brownie", "Desserts", 130)],
    "Desserts": [("Red Velvet Pastry", "Desserts", 160), ("Chocolate Truffle", "Desserts", 180),
                 ("Cheesecake Slice", "Desserts", 220), ("Tiramisu Jar", "Desserts", 240),
                 ("Malai Kulfi", "Desserts", 90), ("Gulab Jamun Box", "Desserts", 200),
                 ("Rasmalai", "Desserts", 150), ("Brownie Sundae", "Desserts", 210),
                 ("Assorted Macarons", "Desserts", 320), ("Banoffee Pie", "Desserts", 250),
                 ("Hot Chocolate", "Beverages", 150), ("Cold Brew", "Beverages", 170)],
    "Healthy Bowls": [("Quinoa Power Bowl", "Mains", 340), ("Grilled Chicken Bowl", "Mains", 380),
                      ("Falafel Mezze Bowl", "Mains", 320), ("Paneer Protein Bowl", "Mains", 350),
                      ("Greek Salad", "Starters", 260), ("Hummus and Pita", "Starters", 240),
                      ("Avocado Toast", "Starters", 280), ("Overnight Oats", "Mains", 190),
                      ("Cold Pressed Juice", "Beverages", 160), ("Berry Smoothie", "Beverages", 200),
                      ("Protein Ball", "Desserts", 120), ("Soup of the Day", "Starters", 180)],
}

VEHICLE_MIX = [("bike", 0.70), ("scooter", 0.22), ("car", 0.08)]
TIER_MIX = [("bronze", 0.50), ("silver", 0.35), ("gold", 0.15)]

# tier, base fare, per km, per minute - rates as STRINGS, parsed to Decimal. A float
# literal like 6.5 cannot represent a rate exactly and the error compounds across
# every payout.
TIER_RATES = [
    ("bronze", "20.00", "6.5000", "0.5000"),
    ("silver", "25.00", "7.2500", "0.6000"),
    ("gold", "30.00", "8.0000", "0.7500"),
]

# Monday-first, matching date.weekday(). Normalized in build_demand_curve so the mean is
# exactly 1.0, otherwise orders_per_day would not mean what it says.
DOW_RAW = [0.85, 0.82, 0.90, 1.10, 1.35, 1.45, 1.20]

# Share of a day's orders per hour, index 0 = midnight. Normalized in build_demand_curve.
HOURLY_WEEKDAY_RAW = [0.02, 0.01, 0.01, 0.005, 0.005, 0.01, 0.03, 0.05, 0.08, 0.06, 0.05, 0.15,
                      0.20, 0.12, 0.08, 0.06, 0.08, 0.12, 0.22, 0.25, 0.18, 0.12, 0.08, 0.05]
HOURLY_WEEKEND_RAW = [0.04, 0.03, 0.02, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.07, 0.10, 0.14,
                      0.16, 0.14, 0.10, 0.08, 0.09, 0.12, 0.18, 0.22, 0.24, 0.16, 0.10, 0.07]

# Lognormal, not gaussian: demand is strictly positive and right-skewed.
NOISE_SIGMA = 0.08

# A spike has shape, not a step. The day before picks up 30% of the boost, the day after
# keeps 50% as demand settles.
SHOULDER_BEFORE, SHOULDER_AFTER = 0.30, 0.50

# name, position (fraction of window), peak multiplier, hour window or None, hour focus,
# transit multiplier, surge multiplier, cancel multiplier. One occasion roughly every 3
# weeks; `position` is a fraction of the window so the same catalogue works at 7 days or
# 365. 0.66 not 0.68 for the festival: at 0.68 it landed on a Saturday, already the
# busiest weekday; 0.66 puts it on a Thursday, giving the dataset one genuine mid-week
# demand shock.
EVENTS = [
    ("sports_final", 0.13, 1.80, (18, 22), 2.2, 1.0, 1.0, 1.0),
    ("storm", 0.38, 1.45, None, 1.0, 1.40, 1.60, 2.5),
    ("festival", 0.66, 2.10, None, 1.0, 1.0, 1.0, 1.0),
    ("long_weekend", 0.87, 1.30, None, 1.0, 1.0, 1.0, 1.0),
]
