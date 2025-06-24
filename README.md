This application is a Streamlit-based email management suite.

### Google Analytics

To display website traffic metrics on the **Analytics** page, set the following
environment variables:

```
GA_PROPERTY_ID=<numeric property id>
GA_CREDENTIALS_JSON=<service account JSON string>
GA_MEASUREMENT_ID=<measurement id>
```

The measurement ID enables page view tracking via the gtag script. The property
ID and credentials are required to fetch aggregated statistics.
