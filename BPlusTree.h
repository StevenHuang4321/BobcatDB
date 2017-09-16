#ifndef BPLUSTREE_CC
#define BPLUSTREE_CC
/*
 * Copyright (c) 2015 Srijan R Shetty <srijan.shetty+code@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/* Structure of file
   -----------------
   fileIndex
   leaf
   parent
   nextLeaf
   previousLeaf
   keySize
   key1
   key2
   ...
   keyn
   child1
   child2
   ...
   child (n+1)
   ------------------
   */

/* Conventions
   1. Caller ensures the Node is loaded into memory.
   2. If a function modifies the Node, it saves it back to disk
   */


// #define DEBUG_VERBOSE
// #define DEBUG_NORMAL

// Two modes of running the program, either time it or show output
// #define TIME

#include <chrono>
#include <iostream>
#include <math.h>
#include <fstream>
#include <string>
#include <cstring>
#include <stdlib.h>
#include <queue>
#include <vector>
#include <limits>
#include <algorithm>

using namespace std;


#define DEFAULT_LOCATION -1

namespace BPlusTree {
    // A generic compare function for pairs of numbers
extern string OBJECT_FILE;
extern string TREE_PREFIX;
extern string SESSION_FILE;
extern string CONFIG_FILE;
extern string TABLE_NAME;


    template<typename T>
        class compare {
            public:
                bool operator() (pair<T, double> T1, pair<T, double> T2) {
                    return T1.second > T2.second;
                }
        };

    // Database objects
    class DBObject {
        private:
            double key;
            long fileIndex;
            string dataString;

        public:
            static long objectCount;

        public:
            DBObject(double _key, string _dataString) : key(_key), dataString(_dataString) {
                fileIndex = objectCount++;

                // Open a file and write the string to it
                ofstream ofile(BPlusTree::OBJECT_FILE, ios::app);
                ofile << dataString << endl;
                ofile.close();
            }

            DBObject(double _key, long _fileIndex) : key(_key), fileIndex(_fileIndex) {
                // Open a file and read the dataString
                ifstream ifile(BPlusTree::OBJECT_FILE);
                for (long i = 0; i < fileIndex + 1; ++i) {
                    getline(ifile, dataString);
                }
                ifile.close();
            }

            // Return the key of the object
            double getKey() { return key; }

            // Return the string
            string getDataString() { return dataString; }

            // Return the fileIndex
            long getFileIndex() { return fileIndex; }
    };

    class Node {
        public:
            static long fileCount;              // Count of all files
            static long lowerBound;
            static long upperBound;
            static long pageSize;

        private:
            long fileIndex;                     // Name of file to store contents
            bool leaf;                          // Type of leaf

        public:
            long parentIndex = DEFAULT_LOCATION;
            long nextLeafIndex = DEFAULT_LOCATION;
            long previousLeafIndex = DEFAULT_LOCATION;
            double keyType;                     // Dummy to indicate container base
            vector<double> keys;
            vector<long> childIndices;          // FileIndices of the children
            vector<long> objectPointers;        // To store the object pointers

        public:
            // Basic initialization
            Node();

            // Given a fileIndex, read it
            Node(long _fileIndex);

            // Check if leaf
            bool isLeaf() { return leaf; }

            // Get the file name
            string getFileName() { return TREE_PREFIX + to_string(fileIndex); }

//            string getFileName(string table) { return TREE_PREFIX + table + to_string(fileIndex); }

            // Get the fileIndex
            long getFileIndex() { return fileIndex; }

            // set to internalNode
            void setToInternalNode() { leaf = false; }

            // Return the size of keys
            long size() { return keys.size(); }

            // Initialize the for the tree
            static void initialize();

            // Initialize the for the tree
            static void initialize(string table);

            // Return the position of a key in keys
            long getKeyPosition(double key);

            // Commit node to disk
            void commitToDisk();

            // Read from the disk into memory
            void readFromDisk();

            // Print node information
            void printNode();

            // Serialize the subtree
            void serialize();

            // Insert object into disk
            void insertObject(DBObject object);

            // Insert an internal node into the tree
            void insertNode(double key, long leftChildIndex, long rightChildIndex);

            // Split the current Leaf Node
            void splitLeaf();

            // Split the current internal Node
            void splitInternal();
    };

    extern Node *bRoot;

    // Insert a key into the BPlusTree
    inline void insert(Node *root, DBObject object) {
           // If the root is a leaf, we can directly insert
           if (root->isLeaf()) {
               // Insert object
               root->insertObject(object);

               // Split if required
               if (root->size() > root->upperBound) {
                   root->splitLeaf();
               }

    #ifdef DEBUG_VERBOSE
               // Serialize
               bRoot->serialize();
    #endif
           } else {
               // We traverse the tree
               long position = root->getKeyPosition(object.getKey());

               // Load the node from disk
               Node *nextRoot = new Node(root->childIndices[position]);

               // Recurse into the node
               insert(nextRoot, object);

               // Clean up
               delete nextRoot;
           }
       }

       // Point search in a BPlusTree
    inline void pointQuery(Node *root, double searchKey) {
           // If the root is a leaf, we can directly search
           if (root->isLeaf()) {
               // Print all nodes in the current leaf
               for (long i = 0; i < (long) root->keys.size(); ++i) {
                   if (root->keys[i] == searchKey) {
    #ifdef DEBUG_NORMAL
                       cout << root->keys[i] << " ";
    #endif
    #ifdef OUTPUT
                       cout << DBObject(root->keys[i], root->objectPointers[i]).getDataString() << endl;
    #endif
                   }
               }

               // Check nextleaf for same node
               if (root->nextLeafIndex != DEFAULT_LOCATION) {
                   // Load up the nextLeaf from disk
                   Node *tempNode = new Node(root->nextLeafIndex);

                   // Check in the nextLeaf and delegate
                   if (tempNode->keys.front() == searchKey) {
                       pointQuery(tempNode, searchKey);
                   }

                   delete tempNode;
               }
           } else {
               // We traverse the tree
               long position = root->getKeyPosition(searchKey);

               // Load the node from disk
               Node *nextRoot = new Node(root->childIndices[position]);

               // Recurse into the node
               pointQuery(nextRoot, searchKey);

               // Clean up
               delete nextRoot;
           }
       }

       // window search
       inline vector<DBObject> windowQuery(Node *root, double lowerLimit, double upperLimit) {
       	vector<DBObject> vectorDBObject;
           // If the root is a leaf, we can directly search
           if (root->isLeaf()) {
               // Print all nodes in the current leaf which satisfy the criteria
               for (long i = 0; i < (long) root->keys.size(); ++i) {
                   if (root->keys[i] >= lowerLimit && root->keys[i] <= upperLimit) {
    //#ifdef DEBUG_NORMAL
                        cout << root->keys[i] << " ";
    //#endif
                       vectorDBObject.push_back(DBObject(root->keys[i], root->objectPointers[i]));
                        cout << DBObject(root->keys[i], root->objectPointers[i]).getDataString() << endl;
                   }
               }

               // If the nextLeafNode is not null
               if (root->nextLeafIndex != DEFAULT_LOCATION) {
                   Node *tempNode= new Node(root->nextLeafIndex);

                   // Check for condition and recurse
                   if (tempNode->keys.front() >= lowerLimit && tempNode->keys.front() <=upperLimit) {
                   	vector<DBObject> v = windowQuery(tempNode, lowerLimit, upperLimit);
                   	vectorDBObject.insert(vectorDBObject.end(), v.begin(), v.end());
                   }

                   // Delete the tempNode
                   delete tempNode;
               }
           } else {
               // We traverse the tree
               long position = root->getKeyPosition(lowerLimit);

               // Load the node from disk
               Node *nextRoot = new Node(root->childIndices[position]);

               // Recurse into the node
               vector<DBObject> v = windowQuery(nextRoot, lowerLimit, upperLimit);
               vectorDBObject.insert(vectorDBObject.end(), v.begin(), v.end());

               // Clean up
               delete nextRoot;
           }
           return vectorDBObject;
       }

       //rangesearch
       inline void rangeQuery(Node *root, double center, double range) {
           double upperBound = center + range;
           double lowerBound = (center - range >= 0) ? center - range : 0;

           // Call windowQuery internally
           windowQuery(root, lowerBound, upperBound);
       }

       // kNN query
       inline void kNNQuery(Node *root, double center, long k) {
           // If the root is a leaf, we can directly search
           if (root->isLeaf()) {
               vector< pair<double, long> > answers;

               // We traverse the tree
               long position = root->getKeyPosition(center);

               // Get k keys from ahead
               long count = 0;
               for (long i = position; i < (long)root->keys.size(); ++i, ++count) {
                   answers.push_back(make_pair(root->keys[i], root->objectPointers[i]));
               }

               // Now check for leaves in front
               long nextIndex = root->nextLeafIndex;
               while (count < k && nextIndex != DEFAULT_LOCATION) {
                   Node tempNode = Node(nextIndex);

                   for (long i = 0; i < (long) tempNode.keys.size(); ++i, ++ count) {
                       answers.push_back(make_pair(tempNode.keys[i], tempNode.objectPointers[i]));
                   }

                   // Update the nextIndex
                   nextIndex = tempNode.nextLeafIndex;
               }

               // Get k keys from behind
               count = 0;
               for (long i = 0; i < (long) position; ++i, ++count) {
                   answers.push_back(make_pair(root->keys[i], root->objectPointers[i]));
               }

               // Check for leaves behind
               long previousIndex = root->previousLeafIndex;
               while (count < k && previousIndex != DEFAULT_LOCATION) {
                   Node tempNode = Node(previousIndex);

                   for (long i = 0; i < (long) tempNode.keys.size(); ++i, ++ count) {
                       answers.push_back(make_pair(tempNode.keys[i], tempNode.objectPointers[i]));
                   }

                   // Update the nextIndex
                   previousIndex = tempNode.previousLeafIndex;
               }

               // Sort the obtained answers
               sort(answers.begin(), answers.end(),
                       [&](pair<double, long> T1, pair<double, long> T2) {
                       return (abs(T1.first - center) < abs(T2.first - center));
                       });

               // Print the answers
               pair <double, long> answer;
               for (long i = 0; i < k && i < (long) answers.size(); ++i) {
                   answer = answers[i];
    #ifdef DEBUG_NORMAL
                   cout << answer.first << " ";
    #endif
    #ifdef OUTPUT
                   cout << DBObject(answer.first, answer.second).getDataString() << endl;
    #endif

               }
           } else {
               // We traverse the tree
               long position = root->getKeyPosition(center);

               // Load the node from disk
               Node *nextRoot = new Node(root->childIndices[position]);

               // Recurse into the node
               kNNQuery(nextRoot, center, k);

               // Clean up
               delete nextRoot;
           }
       }

       inline void storeSession() {
           // Create a character buffer which will be written to disk
           long location = 0;
           char buffer[Node::pageSize];

           // Store root's fileIndex
           long fileIndex = bRoot->getFileIndex();
           memcpy(buffer + location, &fileIndex, sizeof(fileIndex));
           location += sizeof(fileIndex);

           // Store the fileCount
           memcpy(buffer + location, &Node::fileCount, sizeof(Node::fileCount));
           location += sizeof(Node::fileCount);

           // Store the objectCount for DBObject
           memcpy(buffer + location, &DBObject::objectCount, sizeof(DBObject::objectCount));
           location += sizeof(DBObject::objectCount);

           // Create a binary file and write to memory
           ofstream sessionFile;
           sessionFile.open(SESSION_FILE + TABLE_NAME, ios::binary|ios::out);
           sessionFile.write(buffer, Node::pageSize);
           sessionFile.close();
       }

       inline void storeSession(string table) {
               // Create a character buffer which will be written to disk
               long location = 0;
               char buffer[Node::pageSize];

               // Store root's fileIndex
               long fileIndex = bRoot->getFileIndex();
               memcpy(buffer + location, &fileIndex, sizeof(fileIndex));
               location += sizeof(fileIndex);

               // Store the fileCount
               memcpy(buffer + location, &Node::fileCount, sizeof(Node::fileCount));
               location += sizeof(Node::fileCount);

               // Store the objectCount for DBObject
               memcpy(buffer + location, &DBObject::objectCount, sizeof(DBObject::objectCount));
               location += sizeof(DBObject::objectCount);

               // Create a binary file and write to memory
               ofstream sessionFile;
               sessionFile.open(SESSION_FILE + TABLE_NAME, ios::binary|ios::out);
               sessionFile.write(buffer, Node::pageSize);
               sessionFile.close();
           }

       inline void loadSession(string table) {
//     		SESSION_FILE += table;
//     		cout<<"SESSION_FILE:"<<SESSION_FILE<<endl;
//     		// Constants
//     		TREE_PREFIX += table;
//     		cout<<"TREE_PREFIX:"<<TREE_PREFIX<<endl;
//     		OBJECT_FILE += table;
//     		cout<<"OBJECT_FILE:"<<OBJECT_FILE<<endl;
             // Create a character buffer which will be written to disk
             long location = 0;
             char buffer[Node::pageSize];

             // Open the binary file ane read into memory
             ifstream sessionFile;
             sessionFile.open(SESSION_FILE, ios::binary|ios::in);
             sessionFile.read(buffer, Node::pageSize);
             sessionFile.close();

             // Retrieve the fileIndex
             long fileIndex;
             memcpy((char *) &fileIndex, buffer + location, sizeof(fileIndex));
             location += sizeof(fileIndex);

             // Retreive the fileCount
             long fileCount;
             memcpy((char *) &fileCount, buffer + location, sizeof(fileCount));
             location += sizeof(fileCount);

             // Retrieve the objectCount
             long objectCount;
             memcpy((char *) &objectCount, buffer + location, sizeof(objectCount));
             location += sizeof(objectCount);

             // Store the session variables
             Node::fileCount = fileCount;
             DBObject::objectCount = objectCount;

             delete bRoot;
             bRoot = new Node(fileIndex);
             bRoot->readFromDisk();
         }

       inline void loadSession() {
           // Create a character buffer which will be written to disk
           long location = 0;
           char buffer[Node::pageSize];

           // Open the binary file ane read into memory
           ifstream sessionFile;
           sessionFile.open(SESSION_FILE + TABLE_NAME, ios::binary|ios::in);
           sessionFile.read(buffer, Node::pageSize);
           sessionFile.close();

           // Retrieve the fileIndex
           long fileIndex;
           memcpy((char *) &fileIndex, buffer + location, sizeof(fileIndex));
           location += sizeof(fileIndex);

           // Retreive the fileCount
           long fileCount;
           memcpy((char *) &fileCount, buffer + location, sizeof(fileCount));
           location += sizeof(fileCount);

           // Retrieve the objectCount
           long objectCount;
           memcpy((char *) &objectCount, buffer + location, sizeof(objectCount));
           location += sizeof(objectCount);

           // Store the session variables
           Node::fileCount = fileCount;
           DBObject::objectCount = objectCount;

           delete bRoot;
           bRoot = new Node(fileIndex);
           bRoot->readFromDisk();
       }

}

/*using namespace BPlusTree;

void buildTree() {
    ifstream ifile;
    ifile.open("./assgn3_bplus_data.txt", ios::in);

    double key;
    string dataString;
    long count = 0;
    while (ifile >> key >> dataString) {
        if (count % 5000 == 0) {
#ifdef DEBUG_NORMAL
            cout << "Inserting " << count << endl;
#endif
        }

        // Insert the object into file
        insert(bRoot, DBObject(key, dataString));

        // Update the counter
        count++;
    }

    // Close the file
    ifile.close();
}

void processQuery() {
    ifstream ifile;
    ifile.open("./assgn3_bplus_querysample.txt", ios::in);

    long query;
    while (ifile >> query) {
        if (query == 0) {
            double key;
            string dataString;
            ifile >> key >> dataString;

#ifdef OUTPUT
            cout << endl << query << " " << key << " " << dataString << endl;
#endif
#ifdef TIME
            cout << query << " ";
            auto start = std::chrono::high_resolution_clock::now();
#endif
            // Insert into the database
            insert(bRoot, DBObject(key, dataString));
#ifdef TIME
            auto elapsed = std::chrono::high_resolution_clock::now() - start;
            long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            cout << microseconds << endl;
#endif
        } else if (query == 1) {
            double key;
            ifile >> key;

#ifdef OUTPUT
            cout << endl << query << " " << key << endl;
#endif
#ifdef TIME
            cout << query << " ";
            auto start = std::chrono::high_resolution_clock::now();
#endif
            // pointQuery
            pointQuery(bRoot, key);
#ifdef TIME
            auto elapsed = std::chrono::high_resolution_clock::now() - start;
            long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            cout << microseconds << endl;
#endif
        } else if (query == 2) {
            double key, range;
            ifile >> key >> range;

#ifdef OUTPUT
            cout << endl << query << " " << key << " " << range << endl;
#endif
#ifdef TIME
            cout << query << " ";
            auto start = std::chrono::high_resolution_clock::now();
#endif
            // rangeQuery
            rangeQuery(bRoot, key, range * 0.1);
#ifdef TIME
            auto elapsed = std::chrono::high_resolution_clock::now() - start;
            long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            cout << microseconds << endl;
#endif
        } else if (query == 3) {
            double key;
            long k;
            ifile >> key >> k;

#ifdef OUTPUT
            cout << endl << query << " " << key << " " << k << endl;
#endif
#ifdef TIME
            cout << query << " ";
            auto start = std::chrono::high_resolution_clock::now();
#endif
            // kNNQuery
            kNNQuery(bRoot, key, k);
#ifdef TIME
            auto elapsed = std::chrono::high_resolution_clock::now() - start;
            long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            cout << microseconds << endl;
#endif
        } else if (query == 4) {
            double lowerLimit;
            double upperLimit;
            ifile >> lowerLimit >> upperLimit;

#ifdef OUTPUT
            cout << endl << query << " " << lowerLimit << " " << upperLimit << endl;
#endif
#ifdef TIME
            cout << query << " ";
            auto start = std::chrono::high_resolution_clock::now();
#endif
            // windowQuery
            windowQuery(bRoot, lowerLimit, upperLimit);
#ifdef TIME
            auto elapsed = std::chrono::high_resolution_clock::now() - start;
            long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            cout << microseconds << endl;
#endif
        }
    }

    // Close the file
    ifile.close();
}

int test() {
    // Initialize the BPlusTree module
    Node::initialize();

    // Create a new tree
    bRoot = new Node();

    // Load session or build a new tree
    ifstream sessionFile(SESSION_FILE);
    if (sessionFile.good()) {
        loadSession();
    } else {
        buildTree();
    }

    // Process queries
    processQuery();

    // Store the session
    storeSession();

    return 0;
}*/
#endif
