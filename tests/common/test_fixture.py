import pytest


@pytest.fixture
def my_fruit():
    return "apple"


@pytest.fixture
def fruit_basket():
    return []


@pytest.fixture
def append_fruit(my_fruit, fruit_basket):
    return fruit_basket.append(my_fruit)


# Append_fruit에서 fruit_basket에 my_fruit을 추가한 값을 캐싱하기 때문에
def test_fruit(my_fruit, fruit_basket, append_fruit): # append_fruit fixture ensures fruit is added
    assert fruit_basket == [my_fruit] # ["apple"] == ["apple"]




@pytest.fixture(autouse=True)
def append_fruit(my_fruit, fruit_basket):
    return fruit_basket.append(my_fruit)


def test(my_fruit, fruit_basket):
    assert fruit_basket == [my_fruit] # ["apple"] == ["apple"]
