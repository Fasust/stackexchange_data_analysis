#!/usr/bin/python

import sys
import subprocess

INVALID_TASKNAME_ERR = "Invalid task name"

def task_switcher(task_name):
    switcher = {
        "wordcount": "wordcount",
        "uniquewords": "uniquewords",
        "morethan10": "morethan10",
        "stopwords": "stopwords",
        "pigtop10": "pigtop10",
        "tags": "tags",
        "counting": "counting",
        "uniqueusers": "uniqueusers",
        "topusers": "topusers",
        "topquestions": "topquestions",
        "favoritequestions": "favoritequestions",
        "averageanswers": "averageanswers",
        "countries": "countries",
        "names": "names",
        "answers": "answers",
        "bigram": "bigram",
        "trigram": "trigram",
        "combiner": "combiner",
        "useless": "useless",
        "searchengine": "searchengine"
    }
    return switcher.get(task_name,INVALID_TASKNAME_ERR)

def task_main_switcher(task_name):
    switcher = {
        "wordcount": "tasks.warmup.wordcount.WordCountMain",
        "uniquewords": "tasks.warmup.uniqueWords.UniqueWordMain",
        "morethan10": "tasks.warmup.morethan10.MoreThan10Main",
        "stopwords": "tasks.warmup.stopwords.StopWordsMain",
        "pigtop10": "pigtop10",
        "tags": "tasks.warmup.tags.UniqueTagsMain",
        "counting": "tasks.discover.counting.CountingMain",
        "uniqueusers": "tasks.discover.uniqueUsers.UniqueUsersMain",
        "topusers": "tasks.discover.topUsers.TopUsersMain",
        "topquestions": "tasks.discover.topQuestions.TopQuestionsMain",
        "favoritequestions": "tasks.discover.favoriteQuestions.FavoriteQuestionsMain",
        "averageanswers": "tasks.discover.averageAnswers.AverageAnswersMain",
        "countries": "tasks.discover.countries.CountriesMain",
        "names": "tasks.discover.names.NamesMain",
        "answers": "tasks.discover.answers.AnswersMain",
        "bigram": "tasks.numbers.bigram.BigramMain",
        "trigram": "tasks.numbers.trigram.TrigramMain",
        "combiner": "tasks.numbers.wordcountcombiner.WordCountCombinerMain",
        "useless": "tasks.numbers.useless.UselessMain",
        "searchengine": "tasks.searchEngine.SearchEngineMain"
    }
    return switcher.get(task_name,INVALID_TASKNAME_ERR)

#If the actual command arguments doesn't match the expected, return.
if len(sys.argv) < 5:
    print "Usage: ./run task_name jar_name input_path output_path"
else:
    #Gather all the required information from the arguments.
    task_name = sys.argv[1]
    jar_name = sys.argv[2]
    input_path = sys.argv[3]
    output_path = sys.argv[4]
    stopwords_path = ""

    #If we are executing the stopwords task, also gather the stopwords file path.
    if task_name == "stopwords":
        if len(sys.argv) < 6:
            print "Usage: ./run task_name jar_name input_path output_path stopwords_path"
        else:
            stopwords_path = sys.argv[5]

    #Check for the validity of the task name.
    if (task_switcher(task_name) == INVALID_TASKNAME_ERR):
        print INVALID_TASKNAME_ERR
    else:
        if task_name == "stopwords": #Add the stopwords file path if necessary.
            runCommand = "hadoop jar " + jar_name + " " + task_main_switcher(task_name) + " " + input_path + " " + output_path + " " + stopwords_path
        else:
            runCommand = "hadoop jar " + jar_name + " " + task_main_switcher(task_name) + " " + input_path + " " + output_path
        #Execute the command.
        process = subprocess.Popen(runCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        print output

    