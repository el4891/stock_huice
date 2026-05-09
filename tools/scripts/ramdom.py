import random
import secrets

def random_from_range(start, end):
    #return random.randint(start, end)
    return secrets.randbelow(end - start + 1) + start

if __name__ == "__main__":
    result = random_from_range(1, 5)
    print(f'result is {result}')
