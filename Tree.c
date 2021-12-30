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
#else
#define ptry(x) do { puts(#x); if ((errno = x) != 0) syserr("Error %d", errno); } while (0)
#endif

#include "Tree.h"

typedef struct {
    HashMap *children;
    pthread_cond_t readlock, writelock;
    int rstate, wstate;
    int rwait, wwait, rrun, wrun;
} Node;

struct Tree {
    Node *root;
    pthread_mutex_t mutex;
};

void print_node(char *curr, const Node *node) {
    const char *key;
    const Node *value;
    for (HashMapIterator it = hmap_iterator(node->children); hmap_next(
            node->children, &it, &key, (void **) &value);) {
        fprintf(stderr, "%s/%s:\n", curr, key);
        strcat(curr, key);
        print_node(curr, value);
    }
}

void print_tree(Tree *tree) {
    ptry(pthread_mutex_lock(&tree->mutex));
    fprintf(stderr, "/:\n");
    char *curr = malloc(10000);
    print_node(curr, tree->root);
    free(curr);
    ptry(pthread_mutex_unlock(&tree->mutex));
}

Node *node_new() {
    Node *n = malloc(sizeof(Node));
    n->children = hmap_new();
    n->rwait = n->wwait = n->rrun = n->wrun = 0;
    n->rstate = n->wstate = 0;
    ptry(pthread_cond_init(&n->readlock, 0));
    ptry(pthread_cond_init(&n->writelock, 0));
    return n;
}

Tree *tree_new() {
    Tree *t = malloc(sizeof(Tree));
    if (t != NULL) {
        t->root = node_new();
        ptry(pthread_mutex_init(&t->mutex, 0));
    }
    return t;
}

void node_free(Node *node) {
    //size_t size = hmap_size(node->children);
    //Node *children[size + 1];
    //size_t i = 0;
    //for (HashMapIterator it = hmap_iterator(node->children); hmap_next(
    //        node->children, &it, NULL, (void **) children + i); i++);
    hmap_free(node->children);
    //for (i = 0; i < size; i++)
    //    node_free(children[i]);
    ptry(pthread_cond_broadcast(&node->writelock));
    ptry(pthread_cond_destroy(&node->writelock));
    ptry(pthread_cond_broadcast(&node->readlock));
    ptry(pthread_cond_destroy(&node->readlock));
    free(node);
}

void tree_free(Tree *t) {
    ptry(pthread_mutex_lock(&t->mutex));
    node_free(t->root);
    ptry(pthread_mutex_unlock(&t->mutex));
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

Node *start_read(Tree *tree, const char *path) {
    ptry(pthread_mutex_lock(&tree->mutex));
    Node *current = get_node(tree, path);
    if (current == NULL) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return NULL;
    }
    current->rwait++;
    if (current->wrun != 0 || current->wwait != 0) {
        while (current->rstate == 0)
            ptry(pthread_cond_wait(&current->readlock, &tree->mutex));
        current->rstate--;
    }
    // Jeśli w międzyczasie nasz wierzchołek przestał istnieć, to zachowujemy
    // się tak, jakby od początku nie istniał.
    current = get_node(tree, path);
    if (current == NULL) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return NULL;
    }
    current->rwait--;
    current->rrun++;
    ptry(pthread_mutex_unlock(&tree->mutex));
    return current;
}

void end_read(Tree *tree, Node *current) {
    ptry(pthread_mutex_lock(&tree->mutex));
    current->rrun--;
    if (current->rrun == 0 && current->wwait > 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
}

Node *get_writelock(Tree *tree, const char *path) {
    Node *current = get_node(tree, path);
    if (current == NULL)
        return NULL;
    current->wwait++;
    if (current->rrun > 0 || current->wrun > 0) {
        while (current->wstate == 0)
            ptry(pthread_cond_wait(&current->writelock, &tree->mutex));
        current->wstate--;
    }
    current->wwait--;
    current->wrun++;
    return current;
}

void release_writelock(Node *current) {
    if (current == NULL)
        return;
    current->wrun--;
    if (current->rwait > 0) {
        current->rstate = current->rwait;
        ptry(pthread_cond_broadcast(&current->readlock));
    } else if (current->wwait > 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
}

char *tree_list(Tree *tree, const char *path) {
    if (!is_path_valid(path))
        return NULL;
    Node *current = start_read(tree, path);
    if (current == NULL)
        return NULL;
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

    end_read(tree, current);
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
    int cmp = strcmp(source_parent, target_parent);
    if (cmp == 0) {
        get_writelock(tree, source_parent);
    } else if (cmp > 0) {
        get_writelock(tree, source_parent);
        get_writelock(tree, target_parent);
    } else {
        get_writelock(tree, target_parent);
        get_writelock(tree, source_parent);
    }
    // TODO nie puszczam locków
    Node *source_node = get_node(tree, source_parent),
            *target_node = get_node(tree, target_parent);
    if (source_node == NULL || target_node == NULL) {
        release_writelock(source_node);
        release_writelock(target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    Node *to_move = hmap_get(source_node->children, source_name);
    if (to_move == NULL) {
        release_writelock(source_node);
        release_writelock(target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    if (strcmp(source, target) == 0) {
        release_writelock(source_node);
        release_writelock(target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return 0;
    }
    if (strncmp(source, target, strlen(source)) == 0) {
        release_writelock(source_node);
        release_writelock(target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return -1;
    }
    if (get_node(tree, target) != NULL) {
        release_writelock(source_node);
        release_writelock(target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EEXIST;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    hmap_remove(source_node->children, source_name);
    hmap_insert(target_node->children, dest_name, to_move);
    ptry(pthread_mutex_lock(&tree->mutex));
    if (cmp == 0)
        release_writelock(source_node);
    else {
        release_writelock(source_node);
        release_writelock(target_node);
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(target_parent);
    free(source_parent);
    return 0;
}

int tree_create(Tree *tree, const char *path) {
    ptry(pthread_mutex_lock(&tree->mutex));
    if (!is_path_valid(path)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EINVAL;
    }
    Node *node;
    char name[MAX_FOLDER_NAME_LENGTH + 1];
    char *parent = make_path_to_parent(path, name);
    if (parent == NULL) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EEXIST;
    }
    get_writelock(tree, parent);
    // TODO nie puszczam locków
    if ((node = get_node(tree, parent)) == NULL) {
        release_writelock(node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    if (get_node(tree, path) != NULL) {
        release_writelock(node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EEXIST;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    Node *new = node_new();
    hmap_insert(node->children, name, new);
    ptry(pthread_mutex_lock(&tree->mutex));
    release_writelock(node);
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
    get_writelock(tree, parent);
    if ((node = get_node(tree, parent)) == NULL ||
        hmap_get(node->children, name) == NULL) {
        release_writelock(node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOENT;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    Node *old = hmap_get(node->children, name);
    if (hmap_size(old->children) != 0) {
        ptry(pthread_mutex_lock(&tree->mutex));
        release_writelock(node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return ENOTEMPTY;
    }
    node_free(old);
    hmap_remove(node->children, name);
    ptry(pthread_mutex_lock(&tree->mutex));
    release_writelock(node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(parent);
    return 0;
}