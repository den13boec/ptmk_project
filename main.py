# libraries
# main: psycopg2 for PostgreSQL, faker for data generation
import string
import psycopg2
from datetime import datetime
import time
import sys
from faker import Faker
from decouple import config

fake = Faker()


# Db connection class
class Database:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=config("db_name"),
            user=config("db_user"),
            password=config("db_password"),
            host=config("db_host"),
            port=config("db_port"),
        )
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def execute_query(self, query, params=None):
        self.cursor.execute(query, params)

    def fetch_all(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.connection.close()


# Employee class to handle employee data
class Employee:
    def __init__(self, full_name, birth_date, gender):
        self.full_name = full_name
        self.birth_date = birth_date
        self.gender = gender

    def save_to_db(self):
        query = """
            INSERT INTO employees (full_name, birth_date, gender)
            VALUES (%s, %s, %s)
        """
        db.execute_query(query, (self.full_name, self.birth_date, self.gender))
        print(f"Employee {self.full_name} added to database.")

    @staticmethod
    def calculate_age(birth_date):
        today = datetime.today().date()
        age = (
            today.year
            - birth_date.year
            - ((today.month, today.day) < (birth_date.month, birth_date.day))
        )
        return age

    @staticmethod
    def batch_save_to_db(employees):
        employee_data = [
            (emp.full_name, emp.birth_date, emp.gender) for emp in employees
        ]

        query = """
            INSERT INTO employees (full_name, birth_date, gender)
            VALUES (%s, %s, %s)
        """
        # executemany for batch insertion
        db.cursor.executemany(query, employee_data)
        db.connection.commit()


# Mode 1: Create employees table
def create_employee_table():
    create_table_query = """
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(255) NOT NULL,
            birth_date DATE NOT NULL,
            gender VARCHAR(10) NOT NULL
        );
    """
    db.execute_query(create_table_query)
    print("Table 'employees' created successfully.")


# Mode 2: Insert employee record
def insert_employee(full_name, birth_date, gender):
    birth_date_converted = datetime.strptime(birth_date, "%Y-%m-%d").date()
    employee = Employee(full_name, birth_date_converted, gender)
    employee.save_to_db()
    print(f"Employee's age: {Employee.calculate_age(birth_date_converted)}")


# Mode 3: List all employees
def list_employees():
    query = """
        SELECT full_name, birth_date, gender
        FROM employees
        GROUP BY full_name, birth_date, gender
        ORDER BY full_name;
    """
    db.execute_query(query)
    employees = db.fetch_all()

    for emp in employees:
        birth_date = emp[1]
        age = Employee.calculate_age(birth_date)
        print(f"Name: {emp[0]}, Birth date: {emp[1]}, Gender: {emp[2]}, Age: {age}")
    print(f"Number of rows: {len(employees)}")


# Mode 4: Generate and insert 1 million employees + 100 employees with last name starting with 'F'
def generate_large_dataset():
    total_employees = 1000000
    print(f"Generating {total_employees} employees...")
    num_males = total_employees // 2
    num_females = total_employees - num_males

    # A-Z
    initial_letters = list(string.ascii_uppercase)

    employees = []
    letter_index = 0

    for i in range(num_males):
        letter = initial_letters[letter_index % len(initial_letters)]
        employees.append(generate_random_employee_with_letter(letter, "Male"))
        letter_index += 1

    letter_index = 0
    for i in range(num_females):
        letter = initial_letters[letter_index % len(initial_letters)]
        employees.append(generate_random_employee_with_letter(letter, "Female"))
        letter_index += 1

    Employee.batch_save_to_db(employees)

    print(
        f"Generated {total_employees} employees with balanced gender and initial letters distribution."
    )

    # 100 employees with 'F' starting letter
    letter_employees = 100

    print(f"Generating {letter_employees} employees...")
    f_employees = [
        generate_random_employee_with_letter("F", "Male")
        for _ in range(letter_employees)
    ]
    Employee.batch_save_to_db(f_employees)

    print(
        f"Generated {letter_employees} employees with last name starting with 'F' and male gender."
    )


def generate_random_employee_with_letter(letter, gender):
    first_name = (
        fake.first_name_male() if gender == "Male" else fake.first_name_female()
    )
    # cutting of first letter for needed one
    last_name = letter + fake.last_name()[1:]

    full_name = f"{last_name} {first_name}"
    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=60)

    return Employee(full_name, birth_date, gender)


# Mode 5: Select male employees with last name starting with 'F' and measure execution time
def select_male_with_f():
    start_time = time.time()
    query = """
        SELECT full_name, birth_date, gender
        FROM employees
        WHERE full_name LIKE 'F%' AND gender = 'Male';
    """
    db.execute_query(query)
    employees = db.fetch_all()
    elapsed_time = time.time() - start_time

    # for emp in employees:
    #     print(f"Name: {emp[0]}, Birth date: {emp[1]}, Gender: {emp[2]}")
    print(f"Number of rows: {len(employees)}")
    print(f"Query executed in {elapsed_time:.4f} seconds.")


# Mode 6: Optimize the database for faster querying
def optimize_database():
    query = """
        CREATE INDEX idx_full_name ON employees (full_name);
    """
    db.execute_query(query)
    print("Index created on full_name columnn.")


# Mode 7: Generate and insert 1 million employees with unchanged real names + 100 employees with last name starting with 'F'
def generate_large_dataset_true_names():
    total_employees = 1000000
    print(f"Generating {total_employees} employees...")
    num_males = total_employees // 2
    num_females = total_employees - num_males

    # A-Z
    initial_letters = list(string.ascii_uppercase)
    total_letters = len(initial_letters)

    # Number of employees needed for each letter
    male_counts = {letter: num_males // total_letters for letter in initial_letters}
    female_counts = {letter: num_females // total_letters for letter in initial_letters}

    # If total number isn't divisible evenly - divide the remainder
    for i in range(num_males % total_letters):
        male_counts[initial_letters[i]] += 1

    for i in range(num_females % total_letters):
        female_counts[initial_letters[i]] += 1

    employees = []

    # Generate employees for each letter, checking quotas.
    # If after 100 attempts we were unable to get a surname
    # for the required letter - replace the first letter manually.
    def generate_employee(gender_counts, gender):
        while sum(gender_counts.values()) > 0:
            for letter in initial_letters:
                if gender_counts[letter] > 0:
                    attempts = 100
                    for _ in range(attempts):
                        last_name = fake.last_name()
                        first_letter = last_name[0].upper()

                        if first_letter == letter:
                            gender_counts[first_letter] -= 1
                            first_name = (
                                fake.first_name_male()
                                if gender == "Male"
                                else fake.first_name_female()
                            )
                            birth_date = fake.date_of_birth(
                                minimum_age=18, maximum_age=60
                            )
                            employees.append(
                                Employee(
                                    f"{last_name} {first_name}", birth_date, gender
                                )
                            )
                            break
                        # If the quota for this letter is 0, skip it
                        if gender_counts.get(first_letter, 0) > 0:
                            gender_counts[first_letter] -= 1
                            first_name = (
                                fake.first_name_male()
                                if gender == "Male"
                                else fake.first_name_female()
                            )
                            birth_date = fake.date_of_birth(
                                minimum_age=18, maximum_age=60
                            )
                            employees.append(
                                Employee(
                                    f"{last_name} {first_name}", birth_date, gender
                                )
                            )
                            continue
                    else:
                        # Forcefully replace the first letter
                        last_name = letter + last_name[1:]
                        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=60)
                        employees.append(
                            Employee(f"{last_name} {first_name}", birth_date, gender)
                        )
                        gender_counts[letter] -= 1
                        break

    generate_employee(male_counts, "Male")
    Employee.batch_save_to_db(employees)

    employees = []
    generate_employee(female_counts, "Female")
    Employee.batch_save_to_db(employees)

    print(
        f"Generated {total_employees} employees with balanced gender and initial letters distribution."
    )

    # 100 employees with 'F' starting letter
    letter_employees = 100
    print(f"Generating {letter_employees} employees...")

    f_employees = [
        generate_employee_with_letter("F", "Male") for _ in range(letter_employees)
    ]
    Employee.batch_save_to_db(f_employees)

    print(
        f"Generated {letter_employees} employees with last name starting with 'F' and male gender."
    )


def generate_employee_with_letter(letter, gender):
    # Generate male names with last name starting with "F"
    while True:
        last_name = fake.last_name()
        if last_name.startswith("F"):
            birth_date = fake.date_of_birth(minimum_age=18, maximum_age=60)
            first_name = (
                fake.first_name_male() if gender == "Male" else fake.first_name_female()
            )
            full_name = f"{last_name} {first_name}"
            return Employee(full_name, birth_date, gender)


if __name__ == "__main__":
    # db connection
    db = Database()

    if len(sys.argv) > 1:
        mode = sys.argv[1]

        if mode == "1":
            # Mode 1: Create employees table
            create_employee_table()

        elif mode == "2" and len(sys.argv) == 5:
            # Mode 2: Insert employee
            insert_employee(sys.argv[2].strip('"'), sys.argv[3], sys.argv[4])

        elif mode == "3":
            # Mode 3: List all employees
            list_employees()

        elif mode == "4":
            # Mode 4: Generate large dataset
            generate_large_dataset()

        elif mode == "5":
            # Mode 5: Select male employees with last name starting with 'F'
            select_male_with_f()

        elif mode == "6":
            # Mode 6: Optimize the database and rerun query
            optimize_database()

        elif mode == "7":
            # Mode 7: Generate large dataset using unchanged generated names
            generate_large_dataset_true_names()

        else:
            print("Invalid mode or missing arguments.")
    else:
        print("Provide a mode to execute (1-7).")

    db.close()
