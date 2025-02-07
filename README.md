# Plata Assignment - Edmund Chu
Repository for Plata Data Engineer Technical Assignment

The task:
You have been provided with a CSV file containing some raw data related to a customer application
process (see below for more details). Note: this is synthetic data and the process described below is
close but not exactly representative of the real process.

The loan application process:
Customers apply for a loan and get quotes for each of our two brands (Bamboo and Plata). We then
run some checks based on the information they have provided. If they pass the checks, we offer
them a loan and they can finalize it, or cancel if they have changed their mind.
If some checks don’t pass, they may be referred to our customer service who will discuss wither
confirm or change the provided information when appropriate (sometimes they don’t put the right
number), or change the parameters of the loan so that they pass the checks (for example,
decreasing the amount if their income is too low)
Over a period of time, customers can make multiple applications, take multiple loans, or “top-up”
their existing loan.
We’ve highlighted in the spreadsheets some values that should help you understand better what
happens and how it is reflected in the data.
Task:
Assuming we get one similar CSV like this every day, containing data for many customers (not just
one like in the provided data) we would like you to build a pipeline to ingest it into our database.
That includes:
- Designing a business-facing data model (i.e. focused on the usage, not necessarily optimized
for pure performance or storage). We let you decide of the appropriate level of
normalization and redundancy, number of tables, etc.
- Creating a DBT model with the appropriate code and relevant testing
- Optional : Setting up an Airflow DAG to run it
In particular, the stakeholder absolutely wants a Customer table presenting all we know about a
customer. We also need to know, for each customer:
- The number of new loans, repeat loans and top-up for both brand for each customer.
- The number of previous applications within brand for each customer

Commented [KM1]: Questions
Commented [KM2]: Probably don’t need to clarify that
questions should be relevant. I’d just say “Asking questions
won’t hurt your chances of progressing further”

Commented [KM3]: Maybe “representative of but not
identical to the real process”?

Commented [KM4]: Providing one aggregated customer
CSV won’t really let people showcase this as it already feels
like we are providing a single customer table.
This may be fine if we want there to be more focus on the
Airflow side
Commented [KM5]: Presumably new loans here means
loans where the status = disbursed

- The reference of the very first loan and application for each customer/brand, and the
reference of the first loan in a series (i.e. when there are top-ups)
We let you decide which table it should be included in.
Deliverables:
- All the code and any other material you find relevant
- An extract of your git repo
Please note:
- There isn’t not necessarily one unique solution.
- No tricks, no traps.
- Use this exercise to show your skills and impress us. If you can do something very efficient
and sophisticated because you are an absolute expert, please do. However don’t over
complicate things just for the sake of it.
- Treat is as you would treat a task in your current job, i.e. with the same level of details,
comments, explanations, quality, testing, version control, etc. The goal is for us to check that
you can complete the task, but also to have an idea of your coding style.
- Feel free to use any database and Airflow instance. You may create one just for this, or work
with already existing ones.