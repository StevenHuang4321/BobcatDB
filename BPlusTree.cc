#include "BPlusTree.h"
namespace BPlusTree {

// Configuration parameters
string CONFIG_FILE = "./bplustree.config";
string SESSION_FILE = "./.tree.session";

// Constants
string TREE_PREFIX = "leaves/leaf_";
string OBJECT_FILE = "objects/objectFile";
string TABLE_NAME = "";

long DBObject::objectCount = 0;

// Initialize static variables
long Node::lowerBound = 0;
long Node::upperBound = 0;
long Node::pageSize = 2048;
long Node::fileCount = 0;

Node *bRoot = nullptr;


Node::Node() {
    // Initially all the fileNames are DEFAULT_LOCATION
    parentIndex = DEFAULT_LOCATION;
    nextLeafIndex = DEFAULT_LOCATION;
    previousLeafIndex = DEFAULT_LOCATION;

    // Initially every node is a leaf
    leaf = true;

    // Exit if the lowerBoundKey is not defined
    if (lowerBound == 0) {
        cout << "LowerKeyBound not defined";
        exit(1);
    }

    // LeafNode properties
    fileIndex = ++fileCount;
}

Node::Node(long _fileIndex) {
    // Exit if the lowerBoundKey is not defined
    if (lowerBound == 0) {
        cout << "LowerKeyBound not defined";
        exit(1);
    }

    // Load the current node from disk
    fileIndex = _fileIndex;
    readFromDisk();
}

void Node::initialize() {
      // Read in the pageSize from the configuration file
//        ifstream configFile;
//        configFile.open(CONFIG_FILE);
//        configFile >> pageSize;
      pageSize = 2048;
      // Save some place in the file for the header
      long headerSize = sizeof(fileIndex)
          + sizeof(leaf)
          + sizeof(parentIndex)
          + sizeof(nextLeafIndex)
          + sizeof(previousLeafIndex);
      pageSize = pageSize - headerSize;

      // Compute parameters
      long nodeSize = sizeof(fileIndex);
      long keySize = sizeof(keyType);
      lowerBound = floor((pageSize - nodeSize) / (2 * (keySize + nodeSize)));
      upperBound = 2 * lowerBound;
      pageSize = pageSize + headerSize;
  }

void Node::initialize(string table) {
      // Read in the pageSize from the configuration file
//        ifstream configFile;
//        configFile.open(CONFIG_FILE);
//        configFile >> pageSize;

	TABLE_NAME = table;
	// Configuration parameters
	 CONFIG_FILE = "./bplustree.config";
	 SESSION_FILE = "./.tree.session";

	// Constants
	 TREE_PREFIX = "leaves/leaf_";
	 OBJECT_FILE = "objects/objectFile";

//		SESSION_FILE += table;
		cout<<"SESSION_FILE:"<<SESSION_FILE<<endl;
		// Constants
//		TREE_PREFIX += table;
		cout<<"TREE_PREFIX:"<<TREE_PREFIX<<endl;
//		OBJECT_FILE += table;
		cout<<"OBJECT_FILE:"<<OBJECT_FILE<<endl;
      pageSize = 2048;
      // Save some place in the file for the header
      long headerSize = sizeof(fileIndex)
          + sizeof(leaf)
          + sizeof(parentIndex)
          + sizeof(nextLeafIndex)
          + sizeof(previousLeafIndex);
      pageSize = pageSize - headerSize;

      // Compute parameters
      long nodeSize = sizeof(fileIndex);
      long keySize = sizeof(keyType);
      lowerBound = floor((pageSize - nodeSize) / (2 * (keySize + nodeSize)));
      upperBound = 2 * lowerBound;
      pageSize = pageSize + headerSize;
  }


long Node::getKeyPosition(double key) {
      // If keys are empty, return
      if (keys.size() == 0 || key <= keys.front()) {
          return 0;
      }

      for (long i = 1; i < (long)keys.size(); ++i) {
          if (keys[i -1] < key && key <= keys[i]) {
              return i;
          }
      }

      return keys.size();
  }

void Node::commitToDisk() {
    // Create a character buffer which will be written to disk
    long location = 0;
    char buffer[pageSize];

    // Store the fileIndex
    memcpy(buffer + location, &fileIndex, sizeof(fileIndex));
    location += sizeof(fileIndex);

    // Add the leaf to memory
    memcpy(buffer + location, &leaf, sizeof(leaf));
    location += sizeof(leaf);

    // Add parent to memory
    memcpy(buffer + location, &parentIndex, sizeof(parentIndex));
    location += sizeof(parentIndex);

    // Add the previous leaf node
    memcpy(buffer + location, &previousLeafIndex, sizeof(previousLeafIndex));
    location += sizeof(previousLeafIndex);

    // Add the next leaf node
    memcpy(buffer + location, &nextLeafIndex, sizeof(nextLeafIndex));
    location += sizeof(nextLeafIndex);

    // Store the number of keys
    long numKeys = keys.size();
    memcpy(buffer + location, &numKeys, sizeof(numKeys));
    location += sizeof(numKeys);

    // Add the keys to memory
    for (auto key : keys) {
        memcpy(buffer + location, &key, sizeof(key));
        location += sizeof(key);
    }

    // Add the child pointers to memory
    if (!leaf) {
        for (auto childIndex : childIndices) {
            memcpy(buffer + location, &childIndex, sizeof(childIndex));
            location += sizeof(childIndex);
        }
    } else {
        for (auto objectPointer : objectPointers) {
            memcpy(buffer + location, &objectPointer, sizeof(objectPointer));
            location += sizeof(objectPointer);
        }
    }

    // Create a binary file and write to memory
    ofstream nodeFile;
    nodeFile.open(getFileName() + TABLE_NAME, ios::binary|ios::out);
    nodeFile.write(buffer, pageSize);
    nodeFile.close();
}


void Node::readFromDisk() {
     // Create a character buffer which will be written to disk
     long location = 0;
     char buffer[pageSize];

     // Open the binary file ane read into memory
     ifstream nodeFile;
     string tmp = getFileName();
     nodeFile.open(getFileName() + TABLE_NAME, ios::binary|ios::in);
     nodeFile.read(buffer, pageSize);
     nodeFile.close();

     // Retrieve the fileIndex
     memcpy((char *) &fileIndex, buffer + location, sizeof(fileIndex));
     location += sizeof(fileIndex);

     // Retreive the type of node
     memcpy((char *) &leaf, buffer + location, sizeof(leaf));
     location += sizeof(leaf);

     // Retrieve the parentIndex
     memcpy((char *) &parentIndex, buffer + location, sizeof(parentIndex));
     location += sizeof(parentIndex);

     // Retrieve the previousLeafIndex
     memcpy((char *) &previousLeafIndex, buffer + location, sizeof(previousLeafIndex));
     location += sizeof(previousLeafIndex);

     // Retrieve the nextLeafIndex
     memcpy((char *) &nextLeafIndex, buffer + location, sizeof(nextLeafIndex));
     location += sizeof(nextLeafIndex);

     // Retrieve the number of keys
     long numKeys;
     memcpy((char *) &numKeys, buffer + location, sizeof(numKeys));
     location += sizeof(numKeys);

     // Retrieve the keys
     keys.clear();
     double key;
     for (long i = 0; i < numKeys; ++i) {
         memcpy((char *) &key, buffer + location, sizeof(key));
         location += sizeof(key);
         keys.push_back(key);
     }

     // Retrieve childPointers
     if (!leaf) {
         childIndices.clear();
         long childIndex;
         for (long i = 0; i < numKeys + 1; ++i) {
             memcpy((char *) &childIndex, buffer + location, sizeof(childIndex));
             location += sizeof(childIndex);
             childIndices.push_back(childIndex);
         }
     } else {
         objectPointers.clear();
         long objectPointer;
         for (long i = 0; i < numKeys; ++i) {
             memcpy((char *) &objectPointer, buffer + location, sizeof(objectPointer));
             location += sizeof(objectPointer);
             objectPointers.push_back(objectPointer);
         }
     }
 }


void Node::printNode() {
      cout << endl << endl;

      cout << "File : " << fileIndex << endl;
      cout << "IsLeaf : " << leaf << endl;
      cout << "Parent : " << parentIndex << endl;
      cout << "PreviousLeaf : " << previousLeafIndex << endl;
      cout << "NextLeaf : " << nextLeafIndex << endl;

      // Print keys
      cout << "Keys : ";
      for (auto key : keys) {
          cout << key << " ";
      }
      cout << endl;

      // Print the name of the child
      cout << "Children : ";
      for (auto childIndex : childIndices) {
          cout << childIndex << " ";
      }
      cout << endl;
  }

void Node::insertObject(DBObject object) {
      long position = getKeyPosition(object.getKey());

      // insert the new key to keys
      keys.insert(keys.begin() + position, object.getKey());

      // insert the object pointer to the end
      objectPointers.insert(objectPointers.begin() + position, object.getFileIndex());

      // Commit the new node back into memory
      commitToDisk();
  }

void Node::serialize() {
     // Return if node is empty
     if (keys.size() == 0) {
         return;
     }

     // Prettify
     cout << endl << endl;

     queue< pair<long, char> > previousLevel;
     previousLevel.push(make_pair(fileIndex, 'N'));

     long currentIndex;
     Node *iterator;
     char type;
     while (!previousLevel.empty()) {
         queue< pair<long, char> > nextLevel;

         while (!previousLevel.empty()) {
             // Get the front and pop
             currentIndex = previousLevel.front().first;
             iterator = new Node(currentIndex);
             type = previousLevel.front().second;
             previousLevel.pop();

             // If it a seperator, print and move ahead
             if (type == '|') {
                 cout << "|| ";
                 continue;
             }

             // Print all the keys
             for (auto key : iterator->keys) {
                 cout << key << " ";
             }

             // Enqueue all the children
             for (auto childIndex : iterator->childIndices) {
                 nextLevel.push(make_pair(childIndex, 'N'));

                 // Insert a marker to indicate end of child
                 nextLevel.push(make_pair(DEFAULT_LOCATION, '|'));
             }

             // Delete allocated memory
             delete iterator;
         }

         // Seperate different levels
         cout << endl << endl;
         previousLevel = nextLevel;
     }
 }

void Node::insertNode(double key, long leftChildIndex, long rightChildIndex) {
     // insert the new key to keys
     long position = getKeyPosition(key);
     keys.insert(keys.begin() + position, key);

     // insert the newChild
     childIndices.insert(childIndices.begin() + position + 1, rightChildIndex);

     // commit changes to disk
     commitToDisk();

#ifdef DEBUG_VERBOSE
     cout << endl;
     cout << "InsertNode : " << endl;
     cout << "Base Node : ";
     for (auto key : keys) {
         cout << key << " ";
     }
     cout << endl;

     // Print them out
     Node *leftChild = new Node(leftChildIndex);
     cout << "LeftNode : ";
     for (auto key : leftChild->keys) {
         cout << key << " ";
     }
     cout << endl;
     delete leftChild;

     Node *rightChild = new Node(rightChildIndex);
     cout << "RightNode : ";
     for (auto key : rightChild->keys) {
         cout << key << " ";
     }
     cout << endl;
     delete rightChild;
#endif

     // If this overflows, we move again upward
     if ((long)keys.size() > upperBound) {
         splitInternal();
     }

     // Update the root if the element was inserted in the root
     if (fileIndex == bRoot->getFileIndex()) {
         bRoot->readFromDisk();
     }
 }

void Node::splitInternal() {
#ifdef DEBUG_VERBOSE
       cout << endl;
       cout << "SplitInternal : " << endl;
       cout << "Base Node : ";
       for (auto key : keys) {
           cout << key << " ";
       }
       cout << endl;
#endif

       // Create a surrogate internal node
       Node *surrogateInternalNode = new Node();
       surrogateInternalNode->setToInternalNode();

       // Fix the keys of the new node
       double startPoint = *(keys.begin() + lowerBound);
       for (auto key = keys.begin() + lowerBound + 1; key != keys.end(); ++key) {
           surrogateInternalNode->keys.push_back(*key);
       }

       // Resize the keys of the current node
       keys.resize(lowerBound);

#ifdef DEBUG_VERBOSE
       // Print them out
       cout << "First InternalNode : ";
       for (auto key : keys) {
           cout << key << " ";
       }
       cout << endl;

       cout << "Second InternalNode : ";
       for (auto key : surrogateInternalNode->keys) {
           cout << key << " ";
       }
       cout << endl;

       cout << "Split At " << startPoint << endl;
#endif

       // Partition children for the surrogateInternalNode
       for (auto childIndex = childIndices.begin() + lowerBound + 1; childIndex != childIndices.end(); ++childIndex) {
           surrogateInternalNode->childIndices.push_back(*childIndex);

           // Assign parent to the children nodes
           Node *tempChildNode = new Node(*childIndex);
           tempChildNode->parentIndex = surrogateInternalNode->fileIndex;
           tempChildNode->commitToDisk();
           delete tempChildNode;
       }

       // Fix children for the current node
       childIndices.resize(lowerBound + 1);

       // If the current node is not a root node
       if (parentIndex != DEFAULT_LOCATION) {
           // Assign parents
           surrogateInternalNode->parentIndex = parentIndex;
           surrogateInternalNode->commitToDisk();
           commitToDisk();

           // Now we push up the splitting one level
           Node *tempParent = new Node(parentIndex);
           tempParent->insertNode(startPoint, fileIndex, surrogateInternalNode->fileIndex);
           delete tempParent;
       } else {
           // Create a new parent node
           Node *newParent = new Node();
           newParent->setToInternalNode();

           // Assign parents
           surrogateInternalNode->parentIndex = newParent->fileIndex;
           parentIndex = newParent->fileIndex;

           // Insert the key into the keys
           newParent->keys.push_back(startPoint);

           // Insert the children
           newParent->childIndices.push_back(fileIndex);
           newParent->childIndices.push_back(surrogateInternalNode->fileIndex);

           // Commit changes to disk
           newParent->commitToDisk();
           commitToDisk();
           surrogateInternalNode->commitToDisk();

           // Clean up the previous root node
           delete bRoot;

           // Reset the root node
           bRoot = newParent;
       }

       // Clean the surrogateInternalNode
       delete surrogateInternalNode;
   }


void Node::splitLeaf() {
#ifdef DEBUG_VERBOSE
       cout << endl;
       cout << "SplitLeaf : " << endl;
       cout << "Base Node : ";

       for (auto key : keys) {
           cout << key << " ";
       }
       cout << endl;
#endif

       // Create a surrogate leaf node with the keys and object Pointers
       Node *surrogateLeafNode = new Node();
       for (long i = lowerBound; i < (long) keys.size(); ++i) {
           DBObject object = DBObject(keys[i], objectPointers[i]);
           surrogateLeafNode->insertObject(object);
       }

       // Resize the current leaf node and commit the node to disk
       keys.resize(lowerBound);
       objectPointers.resize(lowerBound);

#ifdef DEBUG_VERBOSE
       // Print them out
       cout << "First Leaf : ";
       for (auto key : keys) {
           cout << key << " ";
       }
       cout << endl;

       cout << "Second Leaf : ";
       for (auto key : surrogateLeafNode->keys) {
           cout << key << " ";
       }
       cout << endl;
#endif

       // Link up the leaves
       long tempLeafIndex = nextLeafIndex;
       nextLeafIndex = surrogateLeafNode->fileIndex;
       surrogateLeafNode->nextLeafIndex = tempLeafIndex;

       // If the tempLeafIndex is not null we have to load it and set its
       // previous index
       if (tempLeafIndex != DEFAULT_LOCATION) {
           Node *tempLeaf = new Node(tempLeafIndex);
           tempLeaf->previousLeafIndex = surrogateLeafNode->fileIndex;
           tempLeaf->commitToDisk();
           delete tempLeaf;
       }

       surrogateLeafNode->previousLeafIndex = fileIndex;

       // Consider the case when the current node is not a root
       if (parentIndex != DEFAULT_LOCATION) {
           // Assign parents
           surrogateLeafNode->parentIndex = parentIndex;
           surrogateLeafNode->commitToDisk();
           commitToDisk();

           // Now we push up the splitting one level
           Node *tempParent = new Node(parentIndex);
           tempParent->insertNode(surrogateLeafNode->keys.front(), fileIndex, surrogateLeafNode->fileIndex);
           delete tempParent;
       } else {
           // Create a new parent node
           Node *newParent = new Node();
           newParent->setToInternalNode();

           // Assign parents
           surrogateLeafNode->parentIndex = newParent->fileIndex;
           parentIndex = newParent->fileIndex;

           // Insert the key into the keys
           newParent->keys.push_back(surrogateLeafNode->keys.front());

           // Insert the children
           newParent->childIndices.push_back(this->fileIndex);
           newParent->childIndices.push_back(surrogateLeafNode->fileIndex);

           // Commit to disk
           newParent->commitToDisk();
           surrogateLeafNode->commitToDisk();
           commitToDisk();

           // Clean up the root node
           delete bRoot;

           // Reset the root node
           bRoot = newParent;
       }

       // Clean up surrogateNode
       delete surrogateLeafNode;
   }
}

