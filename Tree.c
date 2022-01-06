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
#define printf(...) do{}while(0)
#else
#define ptry(x) do { printf("%d ",__LINE__); puts(#x); if ((errno = x) != 0) syserr("Error %d", errno); } while (0)
#endif

#include "Tree.h"

typedef struct Node {
    HashMap *children;
    pthread_cond_t readlock, writelock, rprio, wprio;
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
        fprintf(stderr, "%s/%s: %p\n", curr, key, value);
        char newcurr[300];
        strcpy(newcurr, curr);
        strcat(newcurr, "/");
        strcat(newcurr, key);
        print_node(newcurr, value);
    }
}

void print_tree(Tree *tree) {
    fprintf(stderr, "/: %p\n", tree->root);
    print_node("", tree->root);
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
    ptry(pthread_cond_init(&n->rprio, 0));
    ptry(pthread_cond_init(&n->wprio, 0));
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

void node_free(Node *node) {
    const char *child_name;
    Node *child;
    for (HashMapIterator it = hmap_iterator(node->children);
         hmap_next(node->children, &it, &child_name, (void **) &child);
         node_free(child));
    hmap_free(node->children);
    ptry(pthread_cond_destroy(&node->writelock));
    ptry(pthread_cond_destroy(&node->readlock));
    ptry(pthread_cond_destroy(&node->rprio));
    ptry(pthread_cond_destroy(&node->wprio));
    free(node);
}

void tree_free(Tree *t) {
    node_free(t->root);
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
    while (current->rstate > 0)
        ptry(pthread_cond_wait(&current->rprio, &tree->mutex));
    if (current->wrun + current->wwait + current->wstate > 0) {
        current->rwait++;
        while (current->rstate == 0)
            ptry(pthread_cond_wait(&current->readlock, &tree->mutex));
        current->rstate--;
        current->rwait--;
        if (current->rstate == 0)
            ptry(pthread_cond_broadcast(&current->rprio));
    }
    current->rrun++;
    if (current->wrun != 0)
        syserr("%d", current->wrun);
}

void release_readlock(Node *current) {
    if (current == NULL)
        return;
    current->rrun--;
    //if (current->rrun < 0)
    //    syserr("r%d %d %d %d", current->rrun, current->rwait, current->wrun,
    //           current->wwait);
    if (current->rrun == 0 && current->wwait > 0 && current->wrun == 0 && current->rstate == 0 && current->wstate == 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
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

bool get_writelock(Tree *tree, Node *current) {
    if (current == NULL)
        return false;
    while (current->wstate > 0)
        ptry(pthread_cond_wait(&current->wprio, &tree->mutex));
    if (current->rrun + current->wrun + current->rstate > 0) {
        current->wwait++;
        while (current->wstate == 0)
            ptry(pthread_cond_wait(&current->writelock, &tree->mutex));
        current->wstate--;
        current->wwait--;
        if (current->wstate == 0)
            ptry(pthread_cond_broadcast(&current->wprio));
    }
    current->wrun++;
    if (current->wrun + current->rrun != 1)
        syserr("%d %d",current->wrun, current->rrun);
    return true;
}

void release_writelock(Node *current) {
    if (current->wrun != 1 || current->rrun > 1)
        syserr("%d %d %d %d", current->wwait, current->wrun, current->rwait,
               current->rrun);
    current->wrun--;
    if (current->rwait > 0 && current->rrun == 0 && current->rstate == 0 && current->wstate == 0) {
        current->rstate = current->rwait;
        ptry(pthread_cond_broadcast(&current->readlock));
    } else if (current->rwait == 0 && current->wwait > 0 && current->rrun == 0 && current->rstate == 0 && current->wstate == 0) {
        current->wstate = 1;
        ptry(pthread_cond_signal(&current->writelock));
    }
}

bool check_readlocks(Node *node) {
    return (node == NULL) || (node->rrun > 0 && check_readlocks(get_father(node)));
}

Node *start_write(Tree *tree, const char *path1, const char *path2) {
    int cmp = strcmp(path1, path2);
    if (cmp < 0) {
        const char *tmp = path1;
        path1 = path2;
        path2 = tmp;
    }
    fprintf(stderr,"sw %s %s\n",path1,path2);
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
            if (get_father(new1) != node1) {
                print_tree(tree);
                syserr("DUPAZUPA %p %p %p", new1, get_father(new1), node1);
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
            if (node2->rrun == 0)
                syserr("rrun");
            release_readlock(node2);
            get_writelock(tree, node2);
            node1->rrun++;
            new1 = hmap_get(node1->children, component1);
            if (new1 == NULL) {
                release_writelock(node2);
                release_held_readlocks(node1, get_father(node2));
                return false;
            }
            has2 = true;
        }
        if (node1 == node2 && subpath1 == NULL) {
            if (node1->rrun == 0)
                syserr("rrun");
            release_readlock(node1);
            get_writelock(tree, node1);
            node2->rrun++;
            new2 = hmap_get(node2->children, component2);
            if (new2 == NULL) {
                release_writelock(node1);
                release_held_readlocks(get_father(new1), node2);
                return false;
            }
            has1 = true;
        }
        node1 = new1;
        node2 = new2;
        if (!check_readlocks(get_father(new1)) || !check_readlocks(get_father(new2))) {
            print_tree(tree);
            fprintf(stderr, "%s %s %p %p\n", path1, path2, new1, new2);
            fprintf(stderr, "%d %d %d", get_father(new1)->wrun, new1->wrun, new2->wrun);
            syserr("%p %p",node1,node2);
        }
        if (subpath1 != NULL)
            subpath1 = split_path(subpath1, component1);
        if (subpath2 != NULL)
            subpath2 = split_path(subpath2, component2);
    }

    // Jeśli tutaj doszliśmy, to zdobycie writelocków prędzej czy później
    // musi się udać.
    if (cmp == 0) {
        if (!has1)
            get_writelock(tree, node1);
    } else {
        if (!has2)
            get_writelock(tree, node2);
        if (!has1)
            get_writelock(tree, node1);
    }
    if (cmp < 0)
        return node2;
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
    printf("l%s\n", path);
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
    fprintf(stderr, "%s e %s\n", source, target);
    Node *help = start_write(tree, source_parent, target_parent);
    if (!help) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        fprintf(stderr, "%s n %s\n", source, target);
        return ENOENT;
    }
    Node *source_node = get_node(tree, source_parent),
            *target_node = get_node(tree, target_parent);
    if (source_node == NULL || target_node == NULL) {
        puts("DUPA");
        print_tree(tree);
        syserr("%s %s %p %p %p", source, target, source_node, target_node,
               help);
    }
    Node *to_move = hmap_get(source_node->children, source_name);
    if (to_move == NULL) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        fprintf(stderr, "%s nm %s\n", source, target);
        return ENOENT;
    }
    if (strcmp(source, target) == 0) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        fprintf(stderr, "%s s %s\n", source, target);
        return 0;
    }
    if (strncmp(source, target, strlen(source)) == 0) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        fprintf(stderr, "%s u %s\n", source, target);
        return -1;
    }
    if (get_node(tree, target) != NULL) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        fprintf(stderr, "%s x %s\n", source, target);
        return EEXIST;
    }
    if (to_move->rrun + to_move->rwait > 0)
        syserr("%d %d %d %d", to_move->rrun, to_move->rwait, to_move->wrun, to_move->wwait);
    to_move->father = target_node;
    //if (source_node->rrun + target_node->rrun > 0)
    //if (strncmp(source_parent, target_parent, strlen(target_parent)) != 0 || target_node->rrun > 1)
    //if (strncmp(source_parent, target_parent, strlen(source_parent)) != 0 || source_node->rrun > 1)
    //    syserr("E %d %d", source_node->rrun, target_node->rrun);
    ptry(pthread_mutex_unlock(&tree->mutex));
    fprintf(stderr, "%s m %s\n", source, target);
    hmap_remove(source_node->children, source_name);
    hmap_insert(target_node->children, dest_name, to_move);
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(source_node, target_node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(target_parent);
    free(source_parent);
    return 0;
}

int tree_create(Tree *tree, const char *path) {
    printf("c%s\n", path);
    ptry(pthread_mutex_lock(&tree->mutex));
    printf("e%s\n", path);
    if (!is_path_valid(path)) {
        printf("e%s\n", path);
        ptry(pthread_mutex_unlock(&tree->mutex));
        return EINVAL;
    }
    Node *node;
    char name[MAX_FOLDER_NAME_LENGTH + 1];
    char *parent = make_path_to_parent(path, name);
    if (parent == NULL) {
        printf("r%s\n", path);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return EEXIST;
    }
    if (!start_write(tree, parent, parent)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return ENOENT;
    }
    node = get_node(tree, parent);
    if (hmap_get(node->children, name) != NULL) {
        end_write(node, node);
        printf("x%s\n", path);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return EEXIST;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    printf("C%s\n", path);
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
        free(parent);
        return ENOENT;
    }
    node = get_node(tree, parent);
    Node *old = hmap_get(node->children, name);
    if (old == NULL) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return ENOENT;
    }
    if (hmap_size(old->children) != 0) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return ENOTEMPTY;
    }
    node_free(old);
    ptry(pthread_mutex_unlock(&tree->mutex));
    printf("r%s\n", path);
    hmap_remove(node->children, name);
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(node, node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(parent);
    return 0;
}