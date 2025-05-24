The AAPP (Armenia-Azerbaijan Peace Process) repository integrates functions for collecting news articles from the websites of the Ministries of Foreign Affairs of Armenia and Azerbaijan, as well as from the websites of the Prime Minister of Armenia and the President of Azerbaijan. It also includes functionality for classifying texts and filtering only those related to the peace process.

This process involves several steps. First, the news texts are converted into dense numerical vectors (embeddings) using sentence-transformers and the all-MiniLM-L6-v2 model. These embeddings are then passed to a pre-trained logistic regression classifier to perform the initial classification. Subsequently, keyword-based filtering is applied to refine the classification results and produce a more conservative final output.

The functions are deployed on Google Cloud Run (Cloud Functions). To use them, you must create a profile, obtain service account credentials, and deploy the app that collects and classifies news content via the Google Cloud Console. Both functions can also be adapted for local use.

These functions are triggered monthly to collect new data, classify it, and append the results to an existing database of Armenian and Azerbaijani texts.

This system serves as the backend for a web app that enables analysis and visualization of peace-related rhetoric in Armenia and Azerbaijan. The app is available here:
👉 https://shiny-app-838111613695.us-central1.run.app
