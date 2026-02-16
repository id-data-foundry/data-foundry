
## 1. Get Tunnel Credentials
    1. Navigate to https://ngrok.com/our-product/secure-tunnels
    2. Click the `Get started for free` button
    3. Create an account or log in
    4. Scroll down and in the second step, copy the auth token (only the token itself)
    5. Open the ngrok.env file in this repository, and paste the auth token
    6. Take a note of the link at the bottom of the same section. This is where you will find your Data Foundry server

## 2. Build Base & DF
    then use the regular steps to build Data Foundry but skip step 6 and come back here.
## 3. Run Docker Compose
To run your new Data Foundry server using ngrok, run the following command
```docker compose -f examples/DF_ngrok/localtest.DF-ngrok.yaml up -d```

## 4. Open your Ngrok link and get started with Data Foundry