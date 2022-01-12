#include "Node.h"
#include <errno.h>
#include <malloc.h>
#include "HashMap.h"
#include <string.h>

// Konwencja używana w synchronizacji:
// z funkcji czytających z hashmapy children (jak find i iteratora)
// można korzystać, jeśli się jest czytelnikiem lub pisarzem danego Node,
// z funkcji modyfukujących hashmapę (jak insert i remove) tylko
// jeśli się jest pisarzem danego node, a z pozostałych zmiennych
// składowych Node można korzystać tylko jeśli się ma mutexa.
typedef struct Node {
    HashMap *children;
    pthread_cond_t readlock, writelock, rprio, wprio;
    int rstate, wstate;
    int rwait, wwait, rrun, wrun;
    struct Node *father;
    int height;
    pthread_mutex_t mutex;
} Node;

Node *get_father(Node *node) {
    if (node == NULL)
        return NULL;
    return node->father;
}

HashMap *get_children(Node *node) {
    return node->children;
}

void set_father(Node *node, Node *father) {
    node->father = father;
}

int get_height(Node *node) {
    if (node == NULL)
        return 0;
    return node->height;
}

Node *node_new(Node *father) {
    Node *n = malloc(sizeof(Node));
    if (n == NULL)
        fatal("Memory allocation failed");
    n->children = hmap_new();
    n->rwait = n->wwait = n->rrun = n->wrun = 0;
    n->rstate = n->wstate = 0;
    n->father = father;
    n->height = get_height(father) + 1;
    ptry(pthread_cond_init(&n->readlock, 0));
    ptry(pthread_cond_init(&n->writelock, 0));
    ptry(pthread_cond_init(&n->rprio, 0));
    ptry(pthread_cond_init(&n->wprio, 0));
    ptry(pthread_mutex_init(&n->mutex, 0));
    return n;
}

void node_free(Node *node) {
    const char *child_name;
    Node *child;
    ptry(pthread_mutex_lock(&node->mutex));
    for (HashMapIterator it = hmap_iterator(node->children);
            hmap_next(node->children, &it,
                      &child_name, (void **) &child);
            node_free(child));
    hmap_free(node->children);
    ptry(pthread_mutex_unlock(&node->mutex));
    ptry(pthread_cond_destroy(&node->writelock));
    ptry(pthread_cond_destroy(&node->readlock));
    ptry(pthread_cond_destroy(&node->rprio));
    ptry(pthread_cond_destroy(&node->wprio));
    ptry(pthread_mutex_destroy(&node->mutex));
    free(node);
}

Node *get_node(Node *root, const char *path) {
    const char *subpath = path;
    char component[MAX_FOLDER_NAME_LENGTH + 1];
    Node *current = root;
    while ((subpath = split_path(subpath, component))) {
        if ((current = hmap_get(current->children, component)) == NULL)
            return NULL;
    }
    return current;
}

void get_readlock(Node *current) {
    if (current == NULL)
        return;
    ptry(pthread_mutex_lock(&current->mutex));
    while (current->rstate > 0)
        ptry(pthread_cond_wait(&current->rprio, &current->mutex));
    if (current->wrun + current->wwait + current->wstate > 0) {
        current->rwait++;
        while (current->rstate == 0)
            ptry(pthread_cond_wait(&current->readlock, &current->mutex));
        current->rstate--;
        current->rwait--;
        if (current->rstate == 0)
            ptry(pthread_cond_broadcast(&current->rprio));
    }
    current->rrun++;
    ptry(pthread_mutex_unlock(&current->mutex));
}

void release_readlock(Node *current) {
    if (current == NULL)
        return;
    ptry(pthread_mutex_lock(&current->mutex));
    current->rrun--;
    if (current->rrun == 0 && current->wwait > 0 && current->wrun == 0 &&
        current->rstate == 0 && current->wstate == 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
    if (current->rwait > 0 && current->rrun == 0 && current->wwait == 0 &&
        current->wrun == 0 && current->rstate == 0 && current->wstate == 0) {
        current->rstate = current->rwait;
        ptry(pthread_cond_broadcast(&current->readlock));
    }
    ptry(pthread_mutex_unlock(&current->mutex));
}

void release_held_readlocks(Node *node1, Node *node2) {
    while (node1 != NULL || node2 != NULL) {
        if (get_height(node1) > get_height(node2)) {
            release_readlock(node1);
            node1 = get_father(node1);
        } else if (get_height(node1) < get_height(node2)) {
            release_readlock(node2);
            node2 = get_father(node2);
        } else {
            release_readlock(node1);
            if (node1 != node2) {
                release_readlock(node2);
            }
            node1 = get_father(node1);
            node2 = get_father(node2);
        }
    }
}

bool start_read(Node *root, const char *path) {
    char component[MAX_FOLDER_NAME_LENGTH + 1];
    Node *node = root;
    const char *subpath = path;
    while ((subpath = split_path(subpath, component))) {
        get_readlock(node);
        Node *new = hmap_get(node->children, component);
        if (new == NULL) {
            release_held_readlocks(node, node);
            return false;
        }
        ptry(pthread_mutex_lock(&new->mutex));
        new->height = node->height + 1;
        ptry(pthread_mutex_unlock(&new->mutex));
        node = new;
    }
    get_readlock(node);
    return true;
}

bool get_writelock(Node *current) {
    if (current == NULL)
        return false;
    ptry(pthread_mutex_lock(&current->mutex));
    while (current->wstate > 0)
        ptry(pthread_cond_wait(&current->wprio, &current->mutex));
    if (current->rrun + current->wrun + current->rstate > 0) {
        current->wwait++;
        while (current->wstate == 0)
            ptry(pthread_cond_wait(&current->writelock, &current->mutex));
        current->wstate--;
        current->wwait--;
        if (current->wstate == 0)
            ptry(pthread_cond_broadcast(&current->wprio));
    }
    current->wrun++;
    ptry(pthread_mutex_unlock(&current->mutex));
    return true;
}

void release_writelock(Node *current) {
    ptry(pthread_mutex_lock(&current->mutex));
    current->wrun--;
    if (current->rwait > 0 && current->rrun == 0 && current->rstate == 0 &&
        current->wstate == 0) {
        current->rstate = current->rwait;
        ptry(pthread_cond_broadcast(&current->readlock));
    }
    if (current->rwait == 0 && current->wwait > 0 && current->rrun == 0 &&
        current->rstate == 0 && current->wstate == 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
    ptry(pthread_mutex_unlock(&current->mutex));
}

bool start_write(Node *root, const char *path1, const char *path2) {
    int cmp = strcmp(path1, path2);
    // Upewniamy się, że path2 nie jest prefixem niewłaściwym path1
    // i jednocześnie zabezpieczamy się przed deadlockiem.
    if (cmp > 0) {
        const char *tmp = path1;
        path1 = path2;
        path2 = tmp;
    }
    char component1[MAX_FOLDER_NAME_LENGTH + 1], component2[
            MAX_FOLDER_NAME_LENGTH + 1];
    Node *node1 = root, *node2 = root;
    const char *subpath1 = path1, *subpath2 = path2;
    while ((subpath1 = split_path(subpath1, component1))) {
        get_readlock(node1);
        Node *new1 = hmap_get(node1->children, component1);
        if (new1 == NULL) {
            release_held_readlocks(node1, node1);
            return false;
        }
        ptry(pthread_mutex_lock(&new1->mutex));
        new1->height = node1->height + 1;
        ptry(pthread_mutex_unlock(&new1->mutex));
        if (node1 == node2) {
            subpath2 = split_path(subpath2, component2);
            if ((node2 = hmap_get(node2->children, component2)) == NULL) {
                release_held_readlocks(node1, node1);
                return false;
            }
            ptry(pthread_mutex_lock(&node2->mutex));
            node2->height = new1->height;
            ptry(pthread_mutex_unlock(&node2->mutex));
        }
        node1 = new1;
    }

    get_writelock(node1);
    while ((subpath2 = split_path(subpath2, component2))) {
        if (node1 == node2) {
            ptry(pthread_mutex_lock(&node2->mutex));
            node2->rrun++;
            ptry(pthread_mutex_unlock(&node2->mutex));
        } else
            get_readlock(node2);
        Node *new = hmap_get(node2->children, component2);
        if (new == NULL) {
            release_writelock(node1);
            release_held_readlocks(get_father(node1), node2);
            return false;
        }
        ptry(pthread_mutex_lock(&new->mutex));
        new->height = node2->height + 1;
        ptry(pthread_mutex_unlock(&new->mutex));
        node2 = new;
    }
    if (cmp != 0)
        get_writelock(node2);
    return true;
}

void end_write(Node *node1, Node *node2) {
    release_writelock(node1);
    if (node1 != node2)
        release_writelock(node2);
    release_held_readlocks(get_father(node1), get_father(node2));
}