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

### Email Settings

Set `SENDER_NAME` to control the publisher portion of the **From** header. The
sender name is automatically formatted as `<Journal Name> – <SENDER_NAME>`.
If not provided, `SENDER_NAME` defaults to `Pushpa Publishing House`.

Example:

```
SENDER_NAME="Pushpa Publishing House"
```
results in a sender like `Far East Journal of Mathematical Sciences (FJMS) – Pushpa Publishing House`.
