import pytest
import assignment_12 as database

file1 = "compressed_friends.txt"
file2 = "compressed_movieratings.txt"

lineage_file1 = "lineage_friends.txt"
lineage_file2 = "lineage_ratings.txt"

responsible_file1 = "responsible_friends.txt"
responsible_file2 = "responsible_ratings.txt"


file_name1 = 'friends.csv'
file_name2 = 'Ratings.csv'

##########################################
########## PULL BASED TESTS ##############
##########################################


def test_lineage_pull_example():
    q2 = database.query2(lineage_file1, lineage_file2, 0, 1)
    lin = q2[0].lineage()
    lin = [elem.tuple for elem in lin]
    assert (0, 1) in lin
    assert (0, 4) in lin
    assert (0, 18) in lin
    assert (1, 10, 5) in lin
    assert (4, 10, 8) in lin
    assert (18, 10, 2) in lin


def test_where_pull_example():
    q2 = database.query2(lineage_file1, lineage_file2, 0, 1)
    where = q2[0].where(0)
    print(where)
    assert ('Ratings.csv', 1, (1, 10, 5), 5) in where
    assert ('Ratings.csv', 2, (4, 10, 8), 8) in where
    assert ('Ratings.csv', 3, (18, 10, 2), 2) in where


def test_how_pull_example():
    q2 = database.query2(lineage_file1, lineage_file2, 0, 1)
    how = q2[0].how()
    assert "AVG( (f1*r1@5), (f2*r2@8), (f3*r3@2) )" in how


def test_responsibility_pull_example():
    q2 = database.query2(responsible_file1, responsible_file2, 0, 1)
    responsible = q2[0].responsible_inputs()
    assert ((0, 1), 0.5) in responsible
    assert ((0, 2), 0.5) in responsible
    assert ((0, 4), 0.5) in responsible
    assert ((1, 10, 5), 0.5) in responsible
    assert ((2, 10, 3), 0.5) in responsible
    assert ((4, 9, 2), 0.5) in responsible
##########################################
########## PUSH BASED TESTS ##############
##########################################


def test_lineage_push_example():
    q2 = database.q2(lineage_file1, lineage_file2, 0, 1, [])
    lin = q2[0].lineage()
    lin = [elem.tuple for elem in lin]
    assert (0, 1) in lin
    assert (0, 4) in lin
    assert (0, 18) in lin
    assert (1, 10, 5) in lin
    assert (4, 10, 8) in lin
    assert (18, 10, 2) in lin


def test_where_push_example():
    q2 = database.q2(lineage_file1, lineage_file2, 0, 1, [])
    where = q2[0].where(0)
    assert ('Ratings.csv', 1, (1, 10, 5), 5) in where
    assert ('Ratings.csv', 2, (4, 10, 8), 8) in where
    assert ('Ratings.csv', 3, (18, 10, 2), 2) in where


def test_how_push_example():
    q2 = database.q2(lineage_file1, lineage_file2, 0, 1, [])
    how = q2[0].how()
    assert "AVG( (f1*r1@5), (f2*r2@8), (f3*r3@2) )" in how


def test_responsibility_push_example():
    q2 = database.q2(responsible_file1, responsible_file2, 0, 1, [])
    responsible = q2[0].responsible_inputs()
    assert ((0, 1), 0.5) in responsible
    assert ((0, 2), 0.5) in responsible
    assert ((0, 4), 0.5) in responsible
    assert ((1, 10, 5), 0.5) in responsible
    assert ((2, 10, 3), 0.5) in responsible
    assert ((4, 9, 2), 0.5) in responsible
