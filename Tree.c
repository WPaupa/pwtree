#include <errno.h>
#include <malloc.h>
#include "HashMap.h"
#include <pthread.h>
#include <string.h>
#include "err.h"
#include "path_utils.h"

#define NDEBUG
#ifdef NDEBUG
#define ptry(x) if ((errno = x) != 0) syserr("Error %d",errno)
//#define printf( ... ) do{}while(0)
#else
#define ptry(x) do { printf("%d ",__LINE__); puts(#x); if ((errno = x) != 0) syserr("Error %d", errno); } while (0)
#endif

#include "Tree.h"

typedef struct Node {
    HashMap *children;
    pthread_cond_t readlock, writelock;
    int rstate, wstate;
    int rwait, wwait, rrun, wrun;
    struct Node *father;
    int height;
} Node;

struct Tree {
    Node *root;
    pthread_mutex_t mutex;
};

void print_node(const char *curr, const Node *node) {
    const char *key;
    const Node *value;
    for (HashMapIterator it = hmap_iterator(node->children); hmap_next(
            node->children, &it, &key, (void **) &value);) {
        fprintf(stderr, "%s/%s:\n", curr, key);
        char newcurr[300];
        strcpy(newcurr, curr);
        strcat(newcurr, "/");
        strcat(newcurr, key);
        print_node(newcurr, value);
    }
}

void print_tree(Tree *tree) {
    ptry(pthread_mutex_lock(&tree->mutex));
    fprintf(stderr, "/:\n");
    print_node("", tree->root);
    ptry(pthread_mutex_unlock(&tree->mutex));
}

Node *get_father(Node *node) {
    if (node == NULL)
        return NULL;
    return node->father;
}

int get_height(Node *node) {
    if (node == NULL)
        return 0;
    return node->height;
}

Node *node_new(Node *father) {
    Node *n = malloc(sizeof(Node));
    n->children = hmap_new();
    n->rwait = n->wwait = n->rrun = n->wrun = 0;
    n->rstate = n->wstate = 0;
    n->father = father;
    n->height = get_height(father) + 1;
    ptry(pthread_cond_init(&n->readlock, 0));
    ptry(pthread_cond_init(&n->writelock, 0));
    return n;
}

Tree *tree_new() {
    Tree *t = malloc(sizeof(Tree));
    if (t != NULL) {
        t->root = node_new(NULL);
        ptry(pthread_mutex_init(&t->mutex, 0));
    }
    return t;
}

void node_free(Tree *tree, Node *node) {
    ptry(pthread_mutex_lock(&tree->mutex));
    hmap_free(node->children);
    node->wstate = node->rstate = -1;
    // Jeśli nie będzie spontanicznego budzenia,
    // to te while wykonają się raz (broadcast
    // wstawi na kolejkę odpowiednie procesy i my
    // zakolejkujemy się za nimi).
    while (node->rwait > 0) {
        ptry(pthread_cond_broadcast(&node->writelock));
        ptry(pthread_mutex_unlock(&tree->mutex));
        ptry(pthread_mutex_lock(&tree->mutex));
    }
    while (node->wwait > 0) {
        ptry(pthread_cond_broadcast(&node->readlock));
        ptry(pthread_mutex_unlock(&tree->mutex));
        ptry(pthread_mutex_lock(&tree->mutex));
    }
    ptry(pthread_cond_destroy(&node->writelock));
    ptry(pthread_cond_destroy(&node->readlock));
    free(node);
    ptry(pthread_mutex_unlock(&tree->mutex));
}

void tree_free(Tree *t) {
    node_free(t, t->root);
    ptry(pthread_mutex_destroy(&t->mutex));
    free(t);
}

Node *get_node(Tree *tree, const char *path) {
    const char *subpath = path;
    char component[MAX_FOLDER_NAME_LENGTH + 1];
    Node *current = tree->root;
    while ((subpath = split_path(subpath, component))) {
        if ((current = hmap_get(current->children, component)) == NULL)
            return NULL;
    }
    return current;
}

void get_readlock(Tree *tree, Node *current) {
    if (current == NULL)
        return;
    current->rwait++;
    if (current->wrun + current->wwait > 0 || current->rstate > 0) {
        while (current->rstate == 0)
            ptry(pthread_cond_wait(&current->readlock, &tree->mutex));
        if (current->rstate == -1) {
            current->rwait--;
            return;
        }
        current->rstate--;
    }
    current->rwait--;
    current->rrun++;
}

void release_readlocks(Node *current, int number) {
    if (current == NULL)
        return;
    current->rrun -= number;
    if (current->rrun == 0 && current->wwait > 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
}

void release_held_readlocks(Node *node1, Node *node2) {
    while (node1 != NULL || node2 != NULL) {
        if (get_height(node1) > get_height(node2)) {
            release_readlocks(node1, 1);
            node1 = get_father(node1);
        } else if (get_height(node1) < get_height(node2)) {
            release_readlocks(node2, 1);
            node2 = get_father(node2);
        } else {
            release_readlocks(node1, 1);
            if (node1 != node2) {
                release_readlocks(node2, 1);
            }
            node1 = get_father(node1);
            node2 = get_father(node2);
        }
    }
}

bool get_writelock(Tree *tree, Node *current) {
    if (current == NULL)
        return false;
    current->wwait++;
    if (current->rrun + current->wrun > 0 || current->wstate > 0) {
        while (current->wstate == 0)
            ptry(pthread_cond_wait(&current->writelock, &tree->mutex));
        // TODO zwolnić co trzeba jak to się nie uda
        if (current->wstate == -1) {
            current->wwait--;
            return false;
        }
        current->wstate--;
    }
    current->wwait--;
    current->wrun++;
    return true;
}

void release_writelock(Node *current) {
    if (current->wrun != 1)
        syserr("%d %d %d %d",current->wwait, current->wrun, current->rwait, current->rrun);
    current->wrun--;
    if (current->rwait > 0) {
        current->rstate = current->rwait;
        ptry(pthread_cond_broadcast(&current->readlock));
    } else if (current->wwait > 0 && current->rrun == 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
}

bool start_write(Tree *tree, const char *path1, const char *path2) {
    char component1[MAX_FOLDER_NAME_LENGTH + 1],
            component2[MAX_FOLDER_NAME_LENGTH + 1];
    bool has1 = false, has2 = false;
    Node *node1 = tree->root, *node2 = tree->root;
    const char *subpath1 = split_path(path1, component1),
            *subpath2 = split_path(path2, component2);
    while (subpath1 || subpath2) {
        Node *new1 = node1, *new2 = node2;
        if (subpath1) {
            get_readlock(tree, node1);
            new1 = hmap_get(node1->children, component1);
            if (new1 == NULL) {
                if (has2)
                    release_writelock(node2);
                release_held_readlocks(node1, get_father(node2));
                return false;
            }
            new1->height = node1->height + 1;
        }
        if (subpath2) {
            if (node1 != node2 || !subpath1)
                get_readlock(tree, node2);
            new2 = hmap_get(node2->children, component2);
            if (new2 == NULL) {
                if (has1)
                    release_writelock(node1);
                release_held_readlocks(get_father(new1), node2);
                return false;
            }
            new2->height = node2->height + 1;
        }

        if (node1 == node2 && subpath2 == NULL) {
            release_readlocks(node2, 1);
            get_writelock(tree, node2);
            node1->rrun++;
            has2 = true;
        }
        if (node1 == node2 && subpath1 == NULL) {
            release_readlocks(node1, 1);
            get_writelock(tree, node1);
            node2->rrun++;
            has1 = true;
        }
        node1 = new1;
        node2 = new2;
        if (subpath1 != NULL)
            subpath1 = split_path(subpath1, component1);
        if (subpath2 != NULL)
            subpath2 = split_path(subpath2, component2);
    }

    // Jeśli tutaj doszliśmy, to zdobycie writelocków prędzej czy później
    // musi się udać.
    int cmp = strcmp(path1, path2);
    if (cmp == 0) {
        if (!has1)
            get_writelock(tree, node1);
    }
    else if (cmp > 0) {
        if (!has1)
            get_writelock(tree, node1);
        if (!has2)
            get_writelock(tree, node2);
    } else {
        if (!has2)
            get_writelock(tree, node2);
        if (!has1)
            get_writelock(tree, node1);
    }
    return node1;
}

void end_write(Node *node1, Node *node2) {
    release_writelock(node1);
    if (node1 != node2)
        release_writelock(node2);
    release_held_readlocks(get_father(node1), get_father(node2));
}

bool start_read(Tree *tree, const char *path) {
    char component[MAX_FOLDER_NAME_LENGTH + 1];
    Node *node = tree->root;
    const char *subpath = path;
    while ((subpath = split_path(subpath, component))) {
        get_readlock(tree, node);
        Node *new = hmap_get(node->children, component);
        if (new == NULL) {
            release_held_readlocks(node, node);
            return false;
        }
        new->height = node->height + 1;
        node = new;
    }
    get_readlock(tree, node);
    return true;
}

char *tree_list(Tree *tree, const char *path) {
    if (!is_path_valid(path))
        return NULL;
    ptry(pthread_mutex_lock(&tree->mutex));
    if (!start_read(tree, path)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return NULL;
    }
    Node *current = get_node(tree, path);
    ptry(pthread_mutex_unlock(&tree->mutex));
    printf("l%s\n",path);
    size_t size = hmap_size(current->children);
    size_t fullsize = 0;
    const char *children[size + 1];
    void *value = NULL;
    size_t n = 0;
    for (HashMapIterator it = hmap_iterator(current->children); hmap_next(
            current->children, &it, children + n, &value); n++) {
        fullsize += strlen(children[n]);
        if (n != 0)
            fullsize++;
    }
    char *result = calloc(fullsize + 1, sizeof(char));
    for (size_t i = 0; i < n; i++) {
        strcat(result, children[i]);
        if (i != n - 1)
            strcat(result, ",");
    }

    pthread_mutex_lock(&tree->mutex);
    release_held_readlocks(current, current);
    pthread_mutex_unlock(&tree->mutex);
    return result;
}

int tree_move(Tree *tree, const char *source, const char *target) {
    if (strcmp(source, "/") == 0)
        return EBUSY;
    if (strcmp(target, "/") == 0)
        return EEXIST;
    ptry(pthread_mutex_lock(&tree->mutex));
    if (!is_path_valid(target) || !is_path_valid(source)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EINVAL;
    }
    char dest_name[MAX_FOLDER_NAME_LENGTH + 1];
    char source_name[MAX_FOLDER_NAME_LENGTH + 1];
    char *target_parent = make_path_to_parent(target, dest_name);
    char *source_parent = make_path_to_parent(source, source_name);
    if (!start_write(tree, source_parent, target_parent)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    Node *source_node = get_node(tree, source_parent),
            *target_node = get_node(tree, target_parent);
    Node *to_move = hmap_get(source_node->children, source_name);
    if (to_move == NULL) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    if (strcmp(source, target) == 0) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return 0;
    }
    if (strncmp(source, target, strlen(source)) == 0) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return -1;
    }
    if (get_node(tree, target) != NULL) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EEXIST;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    printf("%s m %s\n", source, target);
    hmap_remove(source_node->children, source_name);
    hmap_insert(target_node->children, dest_name, to_move);
    to_move->father = target_node;
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(source_node, target_node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(target_parent);
    free(source_parent);
    return 0;
}

int tree_create(Tree *tree, const char *path) {
    printf("c%s\n",path);
    ptry(pthread_mutex_lock(&tree->mutex));
    printf("e%s\n",path);
    if (!is_path_valid(path)) {
        printf("e%s\n",path);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EINVAL;
    }
    Node *node;
    char name[MAX_FOLDER_NAME_LENGTH + 1];
    char *parent = make_path_to_parent(path, name);
    if (parent == NULL) {
        printf("r%s\n",path);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EEXIST;
    }
    if (!start_write(tree, parent, parent)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    node = get_node(tree, parent);
    if (hmap_get(node->children, name) != NULL) {
        end_write(node, node);
        printf("x%s\n",path);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EEXIST;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    printf("C%s\n",path);
    Node *new = node_new(node);
    hmap_insert(node->children, name, new);
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(node, node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(parent);
    return 0;
}

int tree_remove(Tree *tree, const char *path) {
    ptry(pthread_mutex_lock(&tree->mutex));
    if (!is_path_valid(path)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EINVAL;
    }
    if (strcmp(path, "/") == 0) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EBUSY;
    }
    Node *node;
    char name[MAX_FOLDER_NAME_LENGTH + 1];
    char *parent = make_path_to_parent(path, name);
    if (!start_write(tree, parent, parent)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    node = get_node(tree, parent);
    Node *old = hmap_get(node->children, name);
    if (old == NULL) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    if (hmap_size(old->children) != 0) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOTEMPTY;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    printf("r%s\n", path);
    node_free(tree, old);
    hmap_remove(node->children, name);
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(node, node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(parent);
    return 0;
}