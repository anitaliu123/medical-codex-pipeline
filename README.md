# HHA-507-2025
**HHA 507 Data Science Health Informatics 2025**

## Overview
This repository contains course materials, datasets, and code examples for HHA 507 Data Science in Health Informatics. The course focuses on practical data science applications in healthcare, working with medical coding systems, and health data analysis.

## Getting Started

### 1. Cloning the Repository
To get a local copy of this repository on your machine, use the following git commands:

```bash
# Clone the repository to your local machine
git clone https://github.com/yourusername/HHA-507-2025.git

# Navigate into the repository folder
cd HHA-507-2025

# Check the status of your local repository
git status
```

### 2. Basic Git Commands You'll Need
```bash
# Check if there are updates to download: 
git pull
```

### 3. Understanding .gitignore
The `.gitignore` file is crucial for this course because it tells Git which files to ignore and NOT upload to GitHub. This is especially important for:

#### Why We Use .gitignore:
- **Large Dataset Files**: Medical coding datasets (LOINC, ICD-10, HCPCS) can be 100MB+ and exceed GitHub's file size limits
- **Licensing Concerns**: Some medical datasets have usage restrictions and shouldn't be publicly shared
- **Repository Performance**: Keeps the repository lightweight and fast to clone/download
- **Privacy**: Prevents accidental upload of sensitive data files

#### What We're Ignoring:
Our .gitignore specifically excludes these large medical datasets:
- `Module1_MedicalCodexes/loinc/Loinc.csv` - LOINC laboratory codes (~50MB)
- `Module1_MedicalCodexes/icd/us/icd10cm_order_2025.txt` - ICD-10 diagnosis codes
- `Module1_MedicalCodexes/icd/who/icd102019syst_codes.txt` - WHO ICD-10 systematic names
- `Module1_MedicalCodexes/hcpcs/HCPC2025_OCT_ANWEB.txt` - HCPCS procedure codes

## Course Structure

### Module 1: Medical Codexes
Working with standard medical coding systems:
- **ICD-10**: International Classification of Diseases (diagnosis codes)
- **LOINC**: Logical Observation Identifiers Names and Codes (lab tests)
- **HCPCS**: Healthcare Common Procedure Coding System (procedures/supplies)

### Data Sources
Students will need to download these datasets separately from official sources:
- LOINC: https://loinc.org/downloads/
- ICD-10: https://www.cms.gov/medicare/coding-billing/icd-10-codes
- HCPCS: https://www.cms.gov/medicare/coding-billing/hcpcscode

## For Health Informatics Students

### Programming Expectations
This course assumes basic familiarity with:
- Python programming fundamentals
- Working with CSV/text files
- Basic data analysis concepts
- Command line/terminal usage

### Tools You'll Use
- **Python**: Primary programming language
- **Pandas**: Data manipulation and analysis
- **Jupyter Notebooks**: Interactive data exploration
- **Git/GitHub**: Version control and collaboration
- **VS Code**: Code editor

### Tips for Success
1. **Start Early**: Medical datasets are large and complex
2. **Documentation**: Keep detailed notes about your data processing steps
3. **Version Control**: Commit your work frequently with clear messages
4. **Collaboration**: Use GitHub issues and discussions to ask questions
5. **Data Privacy**: Always follow HIPAA and institutional guidelines when working with health data

### Common Issues and Solutions
- **Large File Errors**: If Git complains about file sizes, check that your datasets are properly listed in .gitignore
- **Memory Issues**: Large CSV files may require chunked processing in Python
- **Encoding Problems**: Medical data often uses different character encodings (UTF-8, Latin-1)

