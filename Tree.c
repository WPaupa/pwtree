#include <errno.h>
#include <malloc.h>
#include "HashMap.h"
#include <pthread.h>
#include <string.h>
#include "err.h"
#include "path_utils.h"
#include "sync.h"

#include "Tree.h"

Tree *tree_new() {
    Tree *t = malloc(sizeof(Tree));
    if (t != NULL) {
        t->root = node_new(NULL);
        ptry(pthread_mutex_init(&t->mutex, 0));
    }
    return t;
}

void tree_free(Tree *t) {
    node_free(t->root);
    ptry(pthread_mutex_destroy(&t->mutex));
    free(t);
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
    char *result = make_map_contents_string(get_children(current));

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
        free(target_parent);
        free(source_parent);
        return ENOENT;
    }
    Node *source_node = get_node(tree, source_parent),
            *target_node = get_node(tree, target_parent);
    Node *to_move = hmap_get(get_children(source_node), source_name);
    if (to_move == NULL) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        return ENOENT;
    }
    if (strcmp(source, target) == 0) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        return 0;
    }
    if (strncmp(source, target, strlen(source)) == 0) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        return -1;
    }
    if (get_node(tree, target) != NULL) {
        end_write(source_node, target_node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(target_parent);
        free(source_parent);
        return EEXIST;
    }
    set_father(to_move, target_node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    hmap_remove(get_children(source_node), source_name);
    hmap_insert(get_children(target_node), dest_name, to_move);
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(source_node, target_node);
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
        free(parent);
        return EEXIST;
    }
    if (!start_write(tree, parent, parent)) {
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return ENOENT;
    }
    node = get_node(tree, parent);
    if (hmap_get(get_children(node), name) != NULL) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return EEXIST;
    }
    ptry(pthread_mutex_unlock(&tree->mutex));
    Node *new = node_new(node);
    hmap_insert(get_children(node), name, new);
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
    Node *old = hmap_get(get_children(node), name);
    if (old == NULL) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return ENOENT;
    }
    if (hmap_size(get_children(old)) != 0) {
        end_write(node, node);
        ptry(pthread_mutex_unlock(&tree->mutex));
        free(parent);
        return ENOTEMPTY;
    }
    node_free(old);
    ptry(pthread_mutex_unlock(&tree->mutex));
    hmap_remove(get_children(node), name);
    ptry(pthread_mutex_lock(&tree->mutex));
    end_write(node, node);
    ptry(pthread_mutex_unlock(&tree->mutex));
    free(parent);
    return 0;
}