# Important Actions

## 1. Update the `exchange_rates_usd.json` file daily


Here’s a step-by-step guide to set up a GitHub Action that updates your `exchange_rates_usd.json` file daily:


**Step 1: Create a GitHub Actions Workflow File**

Create a new file in your repo at `.github/workflows/update-exchange-rates.yml`.
Create a new file in your repo at `src/exchange_rates_usd.json`:

```json
{}
```


**Step 2: Add the Workflow YAML**

Paste the following workflow. This example uses Python to fetch rates from exchangerate-api.com and writes them to `src/exchange_rates_usd.json`:

````yaml
name: Update Exchange Rates

on:
  schedule:
    - cron: '0 0 * * *'  # Runs daily at midnight UTC
  workflow_dispatch:      # Allow manual trigger

jobs:
  update-rates:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      - name: Install requests
        run: pip install requests

      - name: Fetch exchange rates and update JSON
        env:
          EXCHANGE_API_KEY: ${{ secrets.EXCHANGE_API_KEY }}
        run: |
          python <<EOF
          import requests, json, os
          url = f"https://v6.exchangerate-api.com/v6/{ '${{ secrets.EXCHANGE_API_KEY }}' }/latest/USD"
          resp = requests.get(url, timeout=10)
          data = resp.json()
          if data.get("result") == "success":
              rates = data.get("conversion_rates", {})
              os.makedirs("src/extractors", exist_ok=True)  # Ensure directory exists
              with open("src/extractors/exchange_rates_usd.json", "w") as f:
                  json.dump(rates, f, indent=2)
          else:
              raise Exception("Failed to fetch rates: " + str(data))
          EOF

      - name: Commit and push if changed
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"
          git add src/exchange_rates_usd.json
          git diff --cached --quiet || (git commit -m "Update exchange rates" && git push)
````

---

**Step 3: Add Your API Key as a GitHub Secret**

1. Go to your repository on GitHub.
2. Click **Settings** > **Secrets and variables** > **Actions**.
3. Click **New repository secret**.
4. Name it `EXCHANGE_API_KEY` and paste your exchangerate-api.com API key as the value.
5. Setup the `github-actions[bot]` permission to push changes to the repository:

#### Setup the `github-actions[bot]` permission to push changes to the repository:
You need to ensure that the GitHub Actions workflow has the correct permissions to push changes to the repository. Here's how you can set it:

#### 1. **Enable Write Permissions for the Workflow**
Make sure that the workflow has permission to write to the repository. You can do this by updating your repository settings:
   - Go to your repository’s **Settings**.
   - Navigate to **Actions** > **General**.
   - Under **Workflow permissions**, select **Read and write permissions**.
   - Ensure that the checkbox for "Allow GitHub Actions to create and approve pull requests" is also checked.

#### 2. **Use a Personal Access Token (Optional)**
If the above step doesn't resolve the issue, you can use a Personal Access Token (PAT) instead of the default `github-actions[bot]`. Follow these steps:
   - Generate a PAT with `repo` scope from your GitHub account.
   - Add the PAT to your repository secrets as `ACTIONS_PAT`.
   - Update the workflow file to use the PAT for authentication:

```yaml
- name: Commit and push if changed
  env:
    ACTIONS_PAT: ${{ secrets.ACTIONS_PAT }}
  run: |
    git config --global user.name "github-actions[bot]"
    git config --global user.email "github-actions[bot]@users.noreply.github.com"
    git add src/extractors/exchange_rates_usd.json
    git diff --cached --quiet || (git commit -m "feat: update exchange rates" && git push https://${ACTIONS_PAT}@github.com/pizofreude/data-career-navigator.git HEAD:main)
```

### Why This Works
- **Write Permissions:** Ensuring the workflow has write permissions allows the `github-actions[bot]` to push changes directly.
- **PAT Authentication:** If `github-actions[bot]` still lacks permissions (e.g., in forks), using a PAT ensures proper authentication for pushing updates.

---

**Step 4: Commit and Push the Workflow**

- Add, commit, and push `.github/workflows/update-exchange-rates.yml` to your repository.

---

**Step 5: Verify**

- The workflow will run daily (or you can trigger it manually from the Actions tab).
- After a successful run, `src/extractors/exchange_rates_usd.json` will be updated with the latest rates and committed to your repo.

---

**Summary of What Happens:**
- The workflow fetches rates from exchangerate-api.com using your secret API key.
- It writes the rates to `src/extractors/exchange_rates_usd.json`.
- If the file changed, it commits and pushes the update.

---
