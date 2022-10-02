# loved_brands_automation
This repo contains an algorithm that identifies vendors whose customers have a higher willingness to pay. The inherent inelasticity of these vendors is utilized as part of a price differentiation strategy called "Loved Brands"

# 1. Introduction to the Strategy
In the **online delivery space**, customers open an app such as Uber Eats and order food, groceries, and other consumables from restaurants/shops. Businesses charge these customers a delivery fee (DF) to get the order delivered to their doorstep. One way to **differentiate the pricing strategy** of these vendors is to identify those who have a **lower elasticity of demand** due to the **customer's loyalty to the brand** and charge customers higher DFs to place orders with said vendors.

The elasticity of demand is one of the most fundamental concepts in pricing and it is calculated using this formula:
```% Change in Orders / % Change in Price```

This implies that customers need to be exposed to various price levels so that the differences in their behavior can be **captured** and **quantified**. This can be done via **continuous experimentation** where the products or services **cycle through different price levels** over **pre-defined time periods** or by leveraging the **inherent price differences** in the standard offerings to consumers. 

### What is the Distance-based Delivery Fee (DBDF) Strategy?
A standard pricing strategy in the **on-demand delivery domain** is charging DFs based on **how far the vendor is from the customer**. The longer the distance, the more the customer has to pay. This strategy offers **more affordable price points** across the willingness to pay spectrum and **protects the profitability per order** by compensating for the higher amount that is paid to the rider for long distance deliveries. The strategy is demonstrated with the infographic below.

![image](https://user-images.githubusercontent.com/98691360/193446308-474bf1ed-b61c-40ae-9e59-6ffbb3154c5e.png)

### How Can the DBDF Strategy be Used to Measure Vendor Elasticity?
These intrinsic price differences can be used to measure the customer's reaction as DFs increase from one tier to the next. Calculating the conversion rate (CVR3) per DF tier generates those so called **elasticity curves** that can be used to detect vendors whose customers have a higher willingness to pay. 

The chart below illustrates this concept with three imaginary vendors. Assuming the **orange** line is the CVR3 per DF tier of the three vendors **combined**, we can construct similar curves for **each vendor individually** and compare **each vendor's performance** to the **overall average (orange line)**. If an individual vendor's curve is **flatter** than the **average curve**, then this vendor has some inherent characteristic that allows them to not exhibit the same CVR3 drops as prices increase. This vendor can then be labelled a **"Loved Brand"**.

![image](https://user-images.githubusercontent.com/98691360/193446426-9d9a8954-5028-481d-b540-108f05cc78a3.png)

### Business Impact of the "Loved Brands" Strategy
Identifying many vendors that exhibit the same behavior and clustering them together gives us the opportunity to capitalize on their lower elasticity and price them up without materially affecting **orders** and **conversion**. This effectively gives the business a second monetization layer on top of the existing distance-based delivery fee strategy. This additional monetization layer can **boost gross profit** and fund **cost increases** and **incentive campaigns** with a muted impact on growth metrics.

The strategy has been proven successful with many AB tests across different markets and has been included as one of the common strategies in our toolbox.

# 2. Usability and Reproducability
This project was an internal project and used proprietary data sources and analysis methodologies. Even though you can clone the repo, the results cannot be reproduced on another machine due to data sharing restrictions. The code will simply give you an error because you don't have the necessary data access permissions.

That said, if you are interested in knowing more about the framework or using something similar to it in a particular use case, feel free to contact me on [LinkedIn](https://www.linkedin.com/in/omar-elmaria/).
